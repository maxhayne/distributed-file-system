package cs555.overlay.node;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.files.ChunkProcessor;
import cs555.overlay.files.ShardProcessor;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.*;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ChunkServer node in the DFS. It is responsible for storing chunks/shards,
 * sending heartbeats to the Controller, serving files, and relaying messages
 * associated with repairing corrupt files.
 *
 * @author hayne
 */
public class ChunkServer implements Node {

  private static final Logger logger = Logger.getInstance();
  private final String host;
  private final int port;
  private final TCPConnectionCache connectionCache;
  private final FileMap files; // map of files stored by server

  // Assigned in registrationSetup()
  private final AtomicBoolean isRegistered;
  private int identifier;
  private Timer heartbeat;
  private FileStreamer streamer;

  record ProcessedFile(String filename, byte[] content, int[] corrupt) {}

  public ChunkServer(String host, int port) {
    this.host = host;
    this.port = port;
    this.connectionCache = new TCPConnectionCache(this); // TODO Dangerous?
    this.files = new FileMap();
    this.isRegistered = new AtomicBoolean(false);
  }

  /**
   * Creates a ServerSocket with an optional port command line argument,
   * connects to the Controller, sends the Controller a registration request,
   * then loops for user commands.
   *
   * @param args port for ServerSocket (optional)
   */
  public static void main(String[] args) {
    // Start the TCPServerThread, so that when we try to register with
    // the Controller, we can guarantee it is already running.

    // If an argument is provided by the user, interpret it as a custom
    // port for the TCPServerThread to run on, and try to use it. Will
    // throw an Exception if the argument is not an integer.
    int serverPort = args.length > 0 ? Integer.parseInt(args[0]) : 0;

    try (ServerSocket serverSocket = new ServerSocket(serverPort)) {

      String host = InetAddress.getLocalHost().getHostAddress();
      ChunkServer chunkServer =
          new ChunkServer(host, serverSocket.getLocalPort());

      // Start the TCPServerThread
      (new Thread(new TCPServerThread(chunkServer, serverSocket))).start();

      logger.info(
          "ServerThread has started at [" + chunkServer.getHost() + ":" +
          chunkServer.getPort() + "]");

      if (chunkServer.registerWithController()) {
        logger.info("Registration request sent to the Controller.");
        // Loop for user commands
        chunkServer.interact();
      }
    } catch (IOException e) {
      logger.error("ChunkServer failed to start. " + e.getMessage());
    } finally {
      System.exit(1);
    }
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public void onEvent(Event event, TCPConnection connection) {
    switch (event.getType()) {
      case Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS:
        registrationInterpreter(event);
        break;

      case Protocol.CONTROLLER_REQUESTS_FILE_DELETE:
        deleteRequestHelper(event, connection);
        break;

      case Protocol.SENDS_FILE_FOR_STORAGE:
        try {
          storeAndRelay(event);
        } catch (NoSuchAlgorithmException e) {
          logger.error("SHA1 unavailable. " + e.getMessage());
        }
        break;

      case Protocol.REQUEST_FILE:
        try {
          serveFile(event, connection);
        } catch (NoSuchAlgorithmException e) {
          logger.error("SHA1 unavailable. " + e.getMessage());
        }
        break;

      case Protocol.CONTROLLER_SENDS_HEARTBEAT:
        acknowledgeHeartbeat(connection);
        break;

      case Protocol.REPAIR_CHUNK:
        try {
          repairChunkHelper(event);
        } catch (NoSuchAlgorithmException e) {
          logger.error("SHA1 unavailable. " + e.getMessage());
        }
        break;

      case Protocol.REPAIR_SHARD:
        try {
          repairShardHelper(event);
        } catch (NoSuchAlgorithmException e) {
          logger.error("SHA1 unavailable. " + e.getMessage());
        }
        break;

      default:
        logger.debug("Event couldn't be processed. " + event.getType());
        break;
    }
  }

  /**
   * Sends a registration request to the Controller.
   *
   * @return true if request was sent, false otherwise
   */
  private boolean registerWithController() {
    GeneralMessage registration =
        new GeneralMessage(Protocol.CHUNK_SERVER_SENDS_REGISTRATION,
            host + ":" + port);
    return connectionCache.send(controllerAddress(), registration, false, true);
  }

  /**
   * Either repairs the local shard or attaches the local shard to the message
   * to be relayed.
   *
   * @param event message being processed
   */
  private void repairShardHelper(Event event) throws NoSuchAlgorithmException {
    RepairShard repairMessage = (RepairShard) event;
    String filename = repairMessage.getFilename();

    // Process the file
    // Either tack the file onto the repair message, or repair the local file
    // Unlock the file for other threads to use

    FileMap.MetaRecord record = files.get(filename); // record is locked
    byte[] fileBytes = streamer.read(filename); // read file
    ShardProcessor processor = new ShardProcessor(fileBytes); // process file

    // repair if we're the destination
    if (repairMessage.getDestination().equals(host + ":" + port)) {
      if (processor.isCorrupt()) {
        boolean repaired = repairAndWriteShard(repairMessage, record.md());
        String success = repaired ? "" : "NOT ";
        logger.debug(filename + " was " + success + "repaired.");
      }
      record.lock().unlock(); // unlock the record
      return;
    }

    record.lock().unlock(); // no longer doing any writing to the file

    // Attach local shard to message
    attachShardToMessage(filename, repairMessage, processor);

    // Relay message to next server
    // TODO Keep trying to forward message if sending to next server fails?
    String nextServer = repairMessage.nextServer();
    connectionCache.send(nextServer, repairMessage, true, false);
  }

  /**
   * Uses the shards attached to the message to attempt to repair the locally
   * corrupt shard, and write it to disk.
   *
   * @param message received from another ChunkServer
   * @param md shard's in-memory metadata
   * @return true if repaired shard was written to disk, false otherwise
   */
  private boolean repairAndWriteShard(RepairShard message, FileMetadata md)
      throws NoSuchAlgorithmException {
    ShardProcessor processor = new ShardProcessor(md, message.getFragments());
    if (!processor.isCorrupt()) {
      return streamer.write(md.getFilename(), processor.getBytes());
    }
    return false;
  }

  /**
   * Attaches local shard to the repair message if the shard isn't corrupt.
   *
   * @param message to attach our shard to
   * @param processor that was used to read the local fragment
   */
  private void attachShardToMessage(String filename, RepairShard message,
      ShardProcessor processor) {
    if (!processor.isCorrupt()) { // if our own shard isn't corrupt
      int shardIndex = FilenameUtilities.getFragment(filename);
      message.attachFragment(shardIndex, processor.getContent());
    }
  }

  /**
   * Either repairs the local chunk or attaches healthy local slices to the
   * message to be relayed.
   *
   * @param event message being processed
   */
  private void repairChunkHelper(Event event) throws NoSuchAlgorithmException {
    RepairChunk repairMessage = (RepairChunk) event;
    String filename = repairMessage.getFilename();

    FileMap.MetaRecord record = files.get(filename); // record is locked
    byte[] fileBytes = streamer.read(filename); // read file
    ChunkProcessor processor = new ChunkProcessor(fileBytes); // process file

    // Process the file
    // Either tack the file onto the repair message, or repair the local file
    // Unlock the record for other threads to use

    // repair if we're the destination
    if (repairMessage.getDestination().equals(host + ":" + port)) {
      if (processor.isCorrupt()) { // And if the chunk is corrupt
        boolean repaired =
            repairAndWriteChunk(repairMessage, processor, record.md());
        String succeeded = repaired ? "" : "NOT ";
        logger.debug(filename + " was " + succeeded + "repaired.");
      }
      record.lock().unlock();
      return;
    }

    record.lock().unlock(); // no longer writing to the file

    // Attach non-corrupt slices and relay the message
    contributeToChunkRepair(repairMessage, processor);

    // Relay message to next server
    // TODO Keep trying to forward message if sending to next server fails
    String nextServer = repairMessage.nextServer();
    connectionCache.send(nextServer, repairMessage,true, false);
  }

  /**
   * Uses slices attached to the message to attempt to repair the locally
   * corrupt chunk, and write it to disk.
   *
   * @param message received from another ChunkServer
   * @param processor used to for processing the chunk
   * @param md chunk's in-memory metadata
   * @return true if successfully wrote repaired chunk to disk, false otherwise
   */
  private boolean repairAndWriteChunk(RepairChunk message,
      ChunkProcessor processor, FileMetadata md)
      throws NoSuchAlgorithmException {
    boolean modified = processor.repair(md, message.getReplacedSlices(),
        message.getRepairedIndices());
    if (modified) {
      return streamer.write(md.getFilename(), processor.getBytes());
    }
    return false;
  }

  /**
   * Attaches local non-corrupt slices of the chunk to the repair message.
   *
   * @param message being added to
   * @param processor that non-corrupt slices are being taken from
   */
  private void contributeToChunkRepair(RepairChunk message,
      ChunkProcessor processor) {
    int[] localCorruptIndices = processor.getCorruptIndices(); // null if none
    byte[][] localSlices = processor.getSlices();
    int[] slicesNeedingRepair = message.slicesStillNeedingRepair();
    for (int index : slicesNeedingRepair) {
      // 'contains' function always returns false if localCorruptSlices is null
      if (!ArrayUtilities.contains(localCorruptIndices, index)) {
        message.attachSlice(index, localSlices[index]);
      }
    }
  }

  /**
   * Acknowledges the Controller's 'poke' heartbeat.
   *
   * @param connection that sent the message (should be the Controller)
   */
  private void acknowledgeHeartbeat(TCPConnection connection) {
    ChunkServerRespondsToHeartbeat ack =
        new ChunkServerRespondsToHeartbeat(identifier);
    try {
      connection.getSender().queueSend(ack.getBytes());
    } catch (IOException e) {
      logger.debug("Can't acknowledge heartbeat. " + e.getMessage());
    }
  }

  /**
   * Attempts to read the requested file from disk, and serves it if it isn't
   * corrupt. If it is corrupt, the Controller is notified, and the request is
   * denied.
   *
   * @param event message being processed
   * @param connection that sent the message
   */
  private void serveFile(Event event, TCPConnection connection)
      throws NoSuchAlgorithmException {
    String filename = ((GeneralMessage) event).getMessage();

    FileMap.MetaRecord record = files.getIfExists(filename); // lock is held

    if (record == null) { // we don't store that file
      try {
        byte messageType = Protocol.CHUNK_SERVER_DENIES_REQUEST;
        sendGeneralMessage(messageType, filename, connection);
      } catch (IOException e) {
        logger.debug("Connection not told of denial. " + e.getMessage());
      }
      return;
    }

    byte[] fileBytes = streamer.read(filename); // read the file
    record.lock().unlock(); // unlock the record
    ProcessedFile processedFile = processFile(filename, fileBytes);

    if (processedFile.content() == null) { // file is corrupt
      ChunkServerReportsFileCorruption message =
          new ChunkServerReportsFileCorruption(identifier, filename,
              processedFile.corrupt());

      connectionCache.send(controllerAddress(), message,true, true);
      try {
        byte messageType = Protocol.CHUNK_SERVER_DENIES_REQUEST;
        sendGeneralMessage(messageType, filename, connection);
      } catch (IOException e) {
        logger.debug("Connection not told of denial. " + e.getMessage());
      }
      return;
    }

    // Since file isn't corrupt, serve it
    ChunkServerServesFile serveMessage =
        new ChunkServerServesFile(filename, processedFile.content());
    try {
      connection.getSender().queueSend(serveMessage.getBytes());
    } catch (IOException e) {
      logger.debug("Unable to serve file to connection. " + e.getMessage());
    }
  }

  /**
   * Processes the fileBytes read from disk either using a Chunk- or
   * ShardProcessor.
   *
   * @param filename name of the file
   * @param fileBytes raw byte[] of file read from disk
   * @return ProcessedFile record containing information about the corruption of
   * the file and the content
   */
  private ProcessedFile processFile(String filename, byte[] fileBytes)
      throws NoSuchAlgorithmException {
    if (FilenameUtilities.checkChunkFilename(filename)) {
      ChunkProcessor processor = new ChunkProcessor(fileBytes);
      if (processor.isCorrupt()) {
        return new ProcessedFile(filename, null, processor.getCorruptIndices());
      }
      return new ProcessedFile(filename, processor.getContent(), null);
    } else if (FilenameUtilities.checkShardFilename(filename)) {
      ShardProcessor processor = new ShardProcessor(fileBytes);
      if (processor.isCorrupt()) {
        return new ProcessedFile(filename, null, null);
      }
      return new ProcessedFile(filename, processor.getContent(), null);
    }
    return new ProcessedFile(filename, null, null);
  }

  /**
   * Stores the file sent for storage, and relays the message to the next
   * ChunkServer.
   *
   * @param event message being processed
   */
  private void storeAndRelay(Event event) throws NoSuchAlgorithmException {
    SendsFileForStorage message = (SendsFileForStorage) event;
    String filename = message.getFilename();

    FileMap.MetaRecord record = files.get(filename); // lock is held
    boolean success = writeFile(record.md(), message); // write the file
    record.lock().unlock(); // unlock the record/file

    // TODO Should the FileMetadata be deleted if the storage operation
    //  failed? No, because the file is intended to be here...

    String not = success ? "" : "NOT ";
    logger.debug(filename + " was " + not + "stored.");

    // Forward the message to next available server
    while (message.nextPosition()) {
      if (connectionCache.send(message.getServer(), message,true, true)) {
        break;
      }
    }
    // TODO Notify the Controller if the file couldn't be stored?
  }

  /**
   * Formats the file content as either a chunk or a shard, and attempts to
   * write it to disk.
   *
   * @param md in-memory metadata of file
   * @param message storage request message
   * @return true if success, false if failure
   */
  private boolean writeFile(FileMetadata md, SendsFileForStorage message)
      throws NoSuchAlgorithmException {
    byte[] preparedBytes;
    String filename = message.getFilename();
    int sequence = FilenameUtilities.getSequence(filename);
    if (FilenameUtilities.checkChunkFilename(filename)) {
      preparedBytes =
          FileUtilities.readyChunkForStorage(sequence, md.getVersion(),
              md.getTimestamp(), message.getContent());
    } else if (FilenameUtilities.checkShardFilename(message.getFilename())) {
      int fragment = FilenameUtilities.getFragment(filename);
      preparedBytes = FileUtilities.readyShardForStorage(sequence, fragment,
          md.getVersion(), md.getTimestamp(), message.getContent());
    } else {
      return false;
    }
    boolean success = streamer.write(filename, preparedBytes);
    if (success) {
      md.updateIfWritten(); // update the metadata
    }
    return success;
  }

  /**
   * Attempts to delete a file with a particular base name from this
   * ChunkServer.
   *
   * @param event message being processed
   * @param connection that sent the message
   */
  private void deleteRequestHelper(Event event, TCPConnection connection) {
    String nameToDelete = ((GeneralMessage) event).getMessage();

    files.getMap().forEachKey(100, (filename) -> {
      if (FilenameUtilities.getBaseFilename(filename).equals(nameToDelete)) {
        files.getMap().computeIfPresent(filename, (name, record) -> {
          record.lock().lock(); // get lock for file
          streamer.delete(name); // delete the file
          // don't need to unlock, the record is no longer in use
          return null; // remove the mapping
        });
      }
    });

    try { // acknowledge request
      sendGeneralMessage(Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE,
          nameToDelete, connection);
    } catch (IOException e) {
      logger.debug("Controller not told of deletion. " + e.getMessage());
    }
  }

  /**
   * Either sets up the ChunkServer, or notifies the user of unsuccessful
   * registration.
   *
   * @param event message being processed
   */
  private void registrationInterpreter(Event event) {
    GeneralMessage report = (GeneralMessage) event;
    int status = Integer.parseInt(report.getMessage());
    if (status == -1) {
      logger.info("Controller denied the registration request.");
    } else {
      boolean setupSuccess = registrationSetup(status);
      if (setupSuccess) {
        logger.info("Controller approved registration. ID: " + status);
      } else {
        logger.info("Failed to set up FileStreamer and HeartbeatService, " +
                    "deregistering.");
        connectionCache.send(controllerAddress(),
            new GeneralMessage(Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION,
                String.valueOf(status)), false, false);
      }
    }
  }

  /**
   * Sets up the ChunkServer.
   *
   * @param id identifier the Controller has assigned this ChunkServer
   * @return true if all actions completed successfully, false otherwise
   */
  private boolean registrationSetup(int id) {
    try {
      identifier = id;
      streamer = new FileStreamer(identifier);
      HeartbeatService heartbeatService = new HeartbeatService(this);
      heartbeat = new Timer();
      long offset = (long) ((Math.random()*(Constants.HEARTRATE/2000)) + 2);
      heartbeat.scheduleAtFixedRate(heartbeatService, offset*1000L,
          Constants.HEARTRATE);
      isRegistered.set(true); // set the registered status
    } catch (Exception e) {
      logger.info("Problem setting up the ChunkServer. " + e.getMessage());
      streamer = null;
      if (heartbeat != null) {
        heartbeat.cancel();
        heartbeat = null;
      }
      isRegistered.set(false);
      return false;
    }
    return true;
  }

  /**
   * Sends a GeneralMessage with specified type and message to the connection
   * passed as a parameter
   *
   * @param type of message
   * @param message string message to send
   * @param connection to send message to
   * @throws IOException if message fails to send
   */
  private void sendGeneralMessage(byte type, String message,
      TCPConnection connection) throws IOException {
    GeneralMessage generalMessage = new GeneralMessage(type, message);
    connection.getSender().queueSend(generalMessage.getBytes());
  }

  /**
   * Loops for user input at the ChunkServer.
   */
  private void interact() {
    System.out.println(
        "Enter a command or use 'help' to print a list of commands.");
    Scanner scanner = new Scanner(System.in);
    interactLoop:
    while (true) {
      String command = scanner.nextLine();
      String[] splitCommand = command.split("\\s+");
      switch (splitCommand[0].toLowerCase()) {

        case "i", "info":
          info();
          break;

        case "f", "files":
          listFiles();
          break;

        case "e", "exit":
          deregister();
          break interactLoop;

        case "h", "help":
          showHelp();
          break;

        default:
          logger.error("Unrecognized command. Use 'help' command.");
          break;
      }
    }
    // Should try to gracefully shut down here
    // Close all TCPConnections
    // Cancel the heartbeat timer
    connectionCache.closeConnections();
    heartbeat.cancel();
    System.exit(0);
  }

  /**
   * Print server address of this ChunkServer.
   */
  private void info() {
    System.out.printf("%3s%s <- %s%n", "", identifier, host + ":" + port);
  }

  /**
   * Send deregistration request to the Controller.
   */
  private void deregister() {
    connectionCache.send(controllerAddress(),
        new GeneralMessage(Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION,
            String.valueOf(identifier)), false, false);
  }

  /**
   * Prints a list of files stored at this ChunkServer. Format is "timestamp
   * version filename" on each line.
   */
  private void listFiles() {
    files.getMap().forEach((name, record) -> {
      System.out.printf("%3s%d %d %s%n", "", record.md().getTimestamp(),
          record.md().getVersion(), name);
    });
  }

  /**
   * Prints a list of valid commands.
   */
  private void showHelp() {
    System.out.printf("%3s%-7s : %s%n", "", "i[nfo]",
        "print host:port server address of this ChunkServer");
    System.out.printf("%3s%-7s : %s%n", "", "f[iles]",
        "print a list of files stored at this ChunkServer");
    System.out.printf("%3s%-7s : %s%n", "", "e[xit]",
        "attempt to deregister and shutdown the ChunkServer");
    System.out.printf("%3s%-7s : %s%n", "", "h[elp]",
        "print a list of valid commands");
  }

  /**
   * Returns the identifier of the ChunkServer (given by the Controller).
   *
   * @return identifier of ChunkServer
   */
  public int getIdentifier() {
    return identifier;
  }

  /**
   * Returns FileMap of files for this ChunkServer.
   *
   * @return files
   */
  public FileMap getFiles() {
    return files;
  }

  /**
   * Returns FileStreamer for this ChunkServer.
   *
   * @return streamer¬
   */
  public FileStreamer getStreamer() {
    return streamer;
  }

  public String controllerAddress() {
    return ApplicationProperties.controllerHost + ":" +
           ApplicationProperties.controllerPort;
  }
}