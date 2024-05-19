package cs555.overlay.node;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.files.ChunkProcessor;
import cs555.overlay.files.FileProcessor;
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
import java.util.List;
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
      serverPort = serverSocket.getLocalPort();
      ChunkServer chunkServer = new ChunkServer(host, serverPort);

      // Start the TCPServerThread
      (new Thread(new TCPServerThread(chunkServer, serverSocket))).start();
      logger.info("ServerThread started at [" + host + ":" + serverPort + "]");

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
        deleteRequestHandler(event, connection);
        break;

      case Protocol.STORE_CHUNK:
        try {
          storeAndRelay(event);
        } catch (NoSuchAlgorithmException e) {
          logger.error("SHA1 unavailable. " + e.getMessage());
        }
        break;

      case Protocol.REQUEST_CHUNK:
        try {
          serveChunk(event);
        } catch (NoSuchAlgorithmException e) {
          logger.error("SHA1 unavailable. " + e.getMessage());
        }
        break;

      case Protocol.CONTROLLER_SENDS_HEARTBEAT:
        acknowledgeHeartbeat(connection);
        break;

      case Protocol.REPAIR_CHUNK:
        try {
          repairChunkHandler(event);
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
    return connectionCache.send(getControllerAddress(), registration, false,
        true);
  }

  /**
   * Either repairs the local chunk, or attaches the (non-corrupt) local chunk
   * to the message, and forwards it.
   *
   * @param event repair being processed
   * @throws NoSuchAlgorithmException
   */
  private void repairChunkHandler(Event event) throws NoSuchAlgorithmException {
    RepairChunk message = (RepairChunk) event;
    String filename = message.getFilenameAtServer();

    FileMap.MetaRecord record = files.get(filename); // lock acquired
    byte[] fileBytes = streamer.read(filename);
    FileProcessor processor;
    if (FilenameUtilities.checkShardFilename(filename)) {
      processor = new ShardProcessor(fileBytes);
    } else {
      processor = new ChunkProcessor(fileBytes);
    }

    // If we are the destination
    if (message.getDestination().equals(host + ":" + port)) {
      if (processor.isCorrupt()) {
        repairAndWriteChunk(message, processor, record.md());
      }
      record.lock().unlock();
      return;
    }
    record.lock().unlock();

    processor.attachToRepair(message);
    forwardRepair(message);
  }

  private void repairAndWriteChunk(RepairChunk message, FileProcessor processor,
      FileMetadata metadata) throws NoSuchAlgorithmException {
    boolean written = false;
    if (processor.repair(metadata, message.getPieces())) {
      written = streamer.write(metadata.getFilename(), processor.getBytes());
    }
    String success = written ? "" : "NOT ";
    logger.debug(metadata.getFilename() + " was " + success + "repaired.");
  }

  /**
   * Forwards the repair message to the next server. If the repair is ready be
   * forwarded directly to the destination, it does that.
   *
   * @param repair message being forwarded
   */
  private void forwardRepair(RepairChunk repair) {
    if (repair.readyToRepair()) {
      repair.prepareForDestination();
      String address = repair.getDestination();
      connectionCache.send(address, repair, true, false);
    } else {
      String nextServer;
      while ((nextServer = repair.getNextAddress()) != null) {
        if (connectionCache.send(nextServer, repair, true, false)) {
          return;
        }
      }
    }
    // TODO Notify the Controller that the chunk couldn't be repaired?
  }

  /**
   * Acknowledges the Controller's 'poke' heartbeat.
   *
   * @param connection that sent the message (should be the Controller)
   */
  private void acknowledgeHeartbeat(TCPConnection connection) {
    ChunkServerRespondsToHeartbeat acknowledge =
        new ChunkServerRespondsToHeartbeat(identifier);
    try {
      connection.getSender().queueSend(acknowledge.getBytes());
    } catch (IOException e) {
      logger.debug("Can't acknowledge heartbeat. " + e.getMessage());
    }
  }

  /**
   * Either serves the chunk to the Client, forwards the request, or notifies
   * both Client and Controller of failure.
   *
   * @param event request being processed
   * @throws NoSuchAlgorithmException if SHA-1 isn't available on this node
   */
  private void serveChunk(Event event) throws NoSuchAlgorithmException {
    RequestChunk request = (RequestChunk) event;
    String filename = request.getFilenameAtServer();

    FileMap.MetaRecord record = files.getIfExists(filename); // lock acquired
    if (record != null) { // this server should have the file
      byte[] fileBytes = streamer.read(filename); // read the file
      FileProcessor processor;
      if (FilenameUtilities.checkChunkFilename(filename)) {
        processor = new ChunkProcessor(fileBytes);
      } else {
        processor = new ShardProcessor(fileBytes);
      }
      processor.attachToRequest(request);
      if (processor.repair(record.md(), request.getPieces())) {
        streamer.write(filename, processor.getBytes());
      }
      if (processor.isCorrupt()) {
        request.addCorruptServer(host + ":" + port);
      }
      record.lock().unlock();
    }

    if (request.readyToServe()) {
      // Serve to the Client, send repairs messages to the corrupt servers
      String baseFilename = request.getFilename();
      byte[] chunkContent = request.getChunk();
      ServeChunk message = new ServeChunk(baseFilename, chunkContent);
      connectionCache.send(request.getClientAddress(), message, true, false);
      sendRepairsToCorruptServers(request);
    } else {
      forwardRequest(request);
    }
  }

  /**
   * Sends repair messages to servers indicated as corrupt in the request.
   *
   * @param request RequestChunk being processed
   */
  private void sendRepairsToCorruptServers(RequestChunk request) {
    String filename = request.getFilename();
    List<String> corruptServers = request.getCorruptServers();
    for (String server : corruptServers) {
      RepairChunk repair =
          new RepairChunk(filename, server, request.getServers());
      repair.setPieces(request.getPieces());
      repair.prepareForDestination();
      connectionCache.send(server, repair, true, false);
    }
  }

  /**
   * Forwards the request to the next server capable of serving the chunk. If
   * not possible, notifies the Controller and Client of failure.
   *
   * @param request RequestChunk being processed
   */
  private void forwardRequest(RequestChunk request) {
    String nextServer;
    while ((nextServer = request.getNextAddress()) != null) {
      if (connectionCache.send(nextServer, request, true, false)) {
        return;
      }
    }
    // Notify the Controller and Client about unrecoverable chunk
    GeneralMessage controllerMessage =
        new GeneralMessage(Protocol.CHUNK_UNRECOVERABLE, request.getFilename());
    connectionCache.send(getControllerAddress(), controllerMessage, true, true);
    GeneralMessage clientMessage =
        new GeneralMessage(Protocol.CHUNK_SERVER_DENIES_REQUEST,
            request.getFilename());
    connectionCache.send(request.getClientAddress(), clientMessage, true,
        false);
  }

  /**
   * Stores the file sent for storage, and relays the message to the next
   * ChunkServer.
   *
   * @param event message being processed
   */
  private void storeAndRelay(Event event) throws NoSuchAlgorithmException {
    StoreChunk message = (StoreChunk) event;
    String filename = message.getFilenameAtServer();

    FileMap.MetaRecord record = files.get(filename); // lock is held
    boolean written = writeFile(record.md(), message); // write the file
    record.lock().unlock(); // unlock the record/file
    if (!written) { // remove the record if the file wasn't written to disk
      files.getMap().computeIfPresent(filename, (name, rec) -> {
        rec.lock().lock();
        return null;
      });
    }

    String not = written ? "" : "NOT ";
    logger.debug(filename + " was " + not + "stored.");

    // Forward the message to next available server
    String nextServer;
    while ((nextServer = message.getNextServer()) != null) {
      if (connectionCache.send(nextServer, message, true, true)) {
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
  private boolean writeFile(FileMetadata md, StoreChunk message)
      throws NoSuchAlgorithmException {
    byte[] fileBytes;
    String filename = message.getFilenameAtServer();
    int sequence = FilenameUtilities.getSequence(filename);
    if (FilenameUtilities.checkChunkFilename(filename)) {
      fileBytes = FileUtilities.readyChunkForStorage(sequence, md.getVersion(),
          md.getTimestamp(), message.getContent());
    } else if (FilenameUtilities.checkShardFilename(filename)) {
      int fragment = FilenameUtilities.getFragment(filename);
      fileBytes = FileUtilities.readyShardForStorage(sequence, fragment,
          md.getVersion(), md.getTimestamp(), message.getContent());
    } else {
      return false;
    }
    boolean success = streamer.write(filename, fileBytes);
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
  private void deleteRequestHandler(Event event, TCPConnection connection) {
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
      byte type = Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE;
      GeneralMessage message = new GeneralMessage(type, nameToDelete);
      connection.getSender().queueSend(message.getBytes());
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
        connectionCache.send(getControllerAddress(),
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
    connectionCache.send(getControllerAddress(),
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

  public String getControllerAddress() {
    return ApplicationProperties.controllerHost + ":" +
           ApplicationProperties.controllerPort;
  }
}