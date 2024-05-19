package cs555.overlay.node;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.transport.*;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.util.HeartbeatInformation;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;

/**
 * Controller node in the DFS. It is responsible for keeping track of registered
 * ChunkServers, deciding where new chunks/shards should be stored, dispatching
 * instructions to deal with file corruptions, and more.
 *
 * @author hayne
 */
public class Controller implements Node {

  private static final Logger logger = Logger.getInstance();
  private final String host;
  private final int port;
  private final TCPConnectionCache connectionCache;
  private final ControllerInformation information;

  public Controller(String host, int port) {
    this.host = host;
    this.port = port;
    this.connectionCache = new TCPConnectionCache(this);// TODO Dangerous?
    this.information = new ControllerInformation(connectionCache);
  }

  /**
   * Entry point for the Controller. Creates a ServerSocket at the port
   * specified in the 'application.properties' file, creates a
   * ControllerInformation (which starts the HeartbeatMonitor), starts looping
   * for user commands.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    try (ServerSocket serverSocket = new ServerSocket(
        ApplicationProperties.controllerPort)) {

      String host = serverSocket.getInetAddress().getHostAddress();
      int serverPort = serverSocket.getLocalPort();
      Controller controller = new Controller(host, serverPort);

      // Start the ServerThread
      (new Thread(new TCPServerThread(controller, serverSocket))).start();
      logger.info("ServerThread started at [" + host + ":" + serverPort + "]");

      // Start the HeartbeatMonitor
      HeartbeatMonitor monitor =
          new HeartbeatMonitor(controller, controller.information);
      Timer heartbeatTimer = new Timer();
      heartbeatTimer.scheduleAtFixedRate(monitor, 0, Constants.HEARTRATE);

      // Start looping for user interaction
      controller.interact();
    } catch (IOException e) {
      logger.error("Controller failed to start. " + e.getMessage());
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

      case Protocol.CHUNK_SERVER_SENDS_REGISTRATION:
        registrationHandler(event, connection, true);
        break;

      case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION:
        registrationHandler(event, connection, false);
        break;

      case Protocol.CLIENT_STORE:
        storeChunk(event, connection);
        break;

      case Protocol.CLIENT_REQUESTS_FILE_DELETE:
        deleteFile(event, connection);
        break;

      case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT:
        heartbeatHandler(event);
        break;

      case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT:
        pokeHandler(event);
        break;

      case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION:
        corruptionHandler(event);
        break;

      case Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO:
        clientRead(event, connection);
        break;

      case Protocol.CLIENT_REQUESTS_FILE_LIST:
        fileListRequest(connection);
        break;

      case Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE:
        logger.debug("ChunkServer acknowledges deletion of " +
                     ((GeneralMessage) event).getMessage());
        break;

      case Protocol.CLIENT_REQUESTS_SERVER_LIST:
        serverListRequest(connection);
        break;

      default:
        logger.debug("Event couldn't be processed. " + event.getType());
        break;
    }
  }

  /**
   * Respond to a request for the list of servers constituting the DFS.
   *
   * @param connection that produced the event
   */
  private void serverListRequest(TCPConnection connection) {
    String servers = String.join("\n", information.serverDetailsList());
    byte type = Protocol.CONTROLLER_SENDS_SERVER_LIST;
    GeneralMessage message = new GeneralMessage(type, servers);
    try {
      connection.getSender().queueSend(message.getBytes());
    } catch (IOException e) {
      logger.debug(
          "Unable to send response to Client containing list of servers. " +
          e.getMessage());
    }
  }

  /**
   * Respond to a request for the list of files stored on the DFS.
   *
   * @param connection that produced the event
   */
  private void fileListRequest(TCPConnection connection) {
    List<String> fileList = information.fileList();
    String[] listToSend = null;
    if (!fileList.isEmpty()) {
      listToSend = new String[fileList.size()];
      fileList.toArray(listToSend);
    }

    ControllerSendsFileList response = new ControllerSendsFileList(listToSend);
    try {
      connection.getSender().queueSend(response.getBytes());
    } catch (IOException e) {
      logger.debug(
          "Unable to send response to Client containing list of files. " +
          e.getMessage());
    }
  }

  /**
   * Gathers information about where a particular file is stored on the DFS, and
   * sends those storage details back to the Client.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private void clientRead(Event event, TCPConnection connection) {
    String filename = ((GeneralMessage) event).getMessage();
    // Get array of servers for all chunks of file
    String[][] servers = information.getFileStorageDetails(filename);

    ControllerSendsStorageList response =
        new ControllerSendsStorageList(filename, servers);
    try {
      connection.getSender().queueSend(response.getBytes());
    } catch (IOException e) {
      logger.debug(
          "Unable to send response to Client containing storage information " +
          "about '" + filename + "'. " + e.getMessage());
    }
  }

  // I've begun to think that the only reason an entry in the String[] server
  // array should be marked null, is when a ChunkServer deregisters, and we
  // cannot find another server to take its place. If we were to mark servers
  // as null when they reported corruption, there is a possibility in the
  // timing that all three servers could be marked null at the same time (not
  // in succession), but that the repairMessages are making the rounds and
  // doing their work properly (especially in the case of slices that are
  // corrupt, but are non-overlapping between servers).

  /**
   * Constructs a message which will be passed (hopefully) amongst those servers
   * that have copies of the file that needs repairing, eventually ending up at
   * the server that produced the corruption event.
   *
   * @param event message being handled
   */
  private synchronized void corruptionHandler(Event event) {
    ChunkServerReportsFileCorruption report =
        (ChunkServerReportsFileCorruption) event;
    String filename = report.getFilename();
    String baseFilename = FilenameUtilities.getBaseFilename(filename);
    int sequence = FilenameUtilities.getSequence(filename);

    String destination =
        information.getChunkServerAddress(report.getIdentifier());
    String[] servers = information.getServers(baseFilename, sequence);

    // If no servers hold this chunk, there is nothing to be done
    if (servers == null) {
      logger.debug("No entry in fileTable for '" + filename +
                   "', so it can't be repaired.");
      return;
    } else if (destination == null) {
      logger.debug("Couldn't get the address of the server to repair.");
      return;
    }

    RepairChunk message =
        ControllerInformation.makeRepairMessage(filename, servers, destination,
            report.getSlices());

    if (message != null) {
      String address = message.getAddress();
      int id = information.getConnection(address).getIdentifier();
      logger.debug("Dispatching repair to " + destination + ", " + id);
      if (connectionCache.send(address, message, true, true)) {
        logger.debug("Sent repair message to " + address);
      } else {
        logger.debug("Unable to send a message to " + address + " to repair '" +
                     report.getFilename() + "' at " + destination);
      }
    }
  }

  /**
   * Update ServerConnection to reflect the fact that it responded to a poke
   * from the Controller.
   *
   * @param event message being handled
   */
  private void pokeHandler(Event event) {
    ChunkServerRespondsToHeartbeat response =
        (ChunkServerRespondsToHeartbeat) event;
    ServerConnection connection =
        information.getConnection(response.getIdentifier());
    if (connection == null) {
      logger.debug("There is no registered ChunkServer with an identifier of " +
                   response.getIdentifier() + ".");
      return;
    }
    connection.incrementPokeReplies();
  }

  /**
   * Update ChunkServer's heartbeat information based on information sent in
   * heartbeat message.
   *
   * @param event message being handled
   */
  private void heartbeatHandler(Event event) {
    ChunkServerSendsHeartbeat heartbeat = (ChunkServerSendsHeartbeat) event;
    ServerConnection connection =
        information.getConnection(heartbeat.getIdentifier());
    if (connection == null) {
      logger.debug("There is no registered ChunkServer with an identifier of " +
                   heartbeat.getIdentifier() + ".");
      return;
    }
    HeartbeatInformation hbi = connection.getHeartbeatInfo();
    hbi.update(heartbeat.getBeatType(), heartbeat.getFreeSpace(),
        heartbeat.getTotalChunks(), heartbeat.getFiles());
  }

  /**
   * Handles requests to delete a file at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private synchronized void deleteFile(Event event, TCPConnection connection) {
    String filename = ((GeneralMessage) event).getMessage();

    // Delete file from the fileTable, and send delete messages to servers too
    information.deleteFileFromDFS(filename);

    // Send client an acknowledgement
    byte type = Protocol.CONTROLLER_APPROVES_FILE_DELETE;
    GeneralMessage response = new GeneralMessage(type, filename);
    try {
      connection.getSender().queueSend(response.getBytes());
    } catch (IOException e) {
      logger.debug("Unable to acknowledge Client's request to delete file. " +
                   e.getMessage());
    }
  }

  /**
   * Handles requests to store a chunk at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private synchronized void storeChunk(Event event, TCPConnection connection) {
    ClientStore request = (ClientStore) event;
    String filename = request.getFilename();
    int sequence = request.getSequence();

    // Check if we've already allocated that particular filename/sequence combo
    String[] servers = information.getServers(filename, sequence);

    // If it wasn't previously allocated, try to allocate it
    if (servers == null) {
      servers = information.allocateServers(filename, sequence);
    }

    // Make correct response
    Event response;
    if (servers == null) {
      byte type = Protocol.CONTROLLER_DENIES_STORAGE_REQUEST;
      response = new GeneralMessage(type, filename);
    } else {
      response = new ControllerReservesServers(filename, sequence, servers);
    }

    try {
      connection.getSender().queueSend(response.getBytes());
    } catch (IOException e) {
      logger.debug("Unable to respond to Client's request to store chunk. " +
                   e.getMessage());
    }
  }

  /**
   * Handles registration requests at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   * @param register true to register, false to deregister
   */
  private synchronized void registrationHandler(Event event,
      TCPConnection connection, boolean register) {
    GeneralMessage request = (GeneralMessage) event;
    if (register) { // Attempt to register
      String address = request.getMessage();
      int registrationStatus = information.register(address, connection);

      byte type = Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS;
      String status = String.valueOf(registrationStatus);
      GeneralMessage message = new GeneralMessage(type, status);
      try {
        connection.getSender().queueSend(message.getBytes());
        if (registrationStatus != -1) {
          // Send under-replicated chunks to new server
          information.refreshServerFiles(registrationStatus);
        }
      } catch (IOException e) {
        logger.debug("Failed to notify ChunkServer of registration status. " +
                     "Deregistering. " + e.getMessage());
        if (registrationStatus != -1) {
          deregister(new ArrayList<>(List.of(registrationStatus)));
        }
      }
    } else { // deregister
      int id = Integer.parseInt(request.getMessage());
      deregister(new ArrayList<>(List.of(id)));
    }
  }

  /**
   * A public deregister method which just takes the identifiers of the servers
   * to be deregistered. Is simple, but needed for the HeartbeatMonitor because
   * it has the proper synchronization.
   *
   * @param identifiers list of the servers to be deregistered
   */
  public synchronized void deregister(ArrayList<Integer> identifiers) {
    information.deregister(identifiers);
  }

  /**
   * Loops for user input to the Controller.
   */
  private void interact() {
    System.out.println(
        "Enter a command or use 'help' to print a list of commands.");
    Scanner scanner = new Scanner(System.in);
    while (true) {
      String command = scanner.nextLine();
      String[] splitCommand = command.split("\\s+");
      switch (splitCommand[0].toLowerCase()) {

        case "s", "servers":
          listRegisteredChunkServers();
          break;

        case "f", "files":
          listAllocatedFiles();
          break;

        case "h", "help":
          showHelp();
          break;

        default:
          System.err.println("Unrecognized command. Use 'help' command.");
          break;
      }
    }
  }

  /**
   * Prints a list of files allocated by the Controller.
   */
  private void listAllocatedFiles() {
    for (String filename : information.fileList()) {
      System.out.printf("%3s%s%n", "", filename);
    }
  }

  /**
   * Prints a list of registered ChunkServers.
   */
  private void listRegisteredChunkServers() {
    for (String serverDetails : information.serverDetailsList()) {
      System.out.printf("%3s%s%n", "", serverDetails);
    }
  }

  /**
   * Prints a list of commands available to the user.
   */
  private void showHelp() {
    System.out.printf("%3s%-9s : %s%n", "", "s[ervers]",
        "print the addresses of all registered ChunkServers");
    System.out.printf("%3s%-9s : %s%n", "", "f[iles]",
        "print the names of all files allocated for storage");
    System.out.printf("%3s%-9s : %s%n", "", "h[elp]",
        "print a list of valid commands");
  }
}