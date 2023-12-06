package cs555.overlay.node;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.transport.ControllerInformation;
import cs555.overlay.transport.ServerConnection;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.util.ForwardInformation;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Scanner;
import java.util.Timer;
import java.util.TreeMap;

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
  private final ControllerInformation information;

  public Controller(String host, int port) {
    this.host = host;
    this.port = port;
    this.information = new ControllerInformation();
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
    try ( ServerSocket serverSocket = new ServerSocket(
        ApplicationProperties.controllerPort ) ) {

      String host = serverSocket.getInetAddress().getHostAddress();
      Controller controller =
          new Controller( host, ApplicationProperties.controllerPort );

      // Start the ServerThread
      (new Thread( new TCPServerThread( controller, serverSocket ) )).start();

      logger.info( "ServerThread has started at ["+host+":"+
                   ApplicationProperties.controllerPort+"]" );

      // Start the HeartbeatMonitor
      HeartbeatMonitor heartbeatMonitor =
          new HeartbeatMonitor( controller, controller.information );
      Timer heartbeatTimer = new Timer();
      heartbeatTimer.scheduleAtFixedRate( heartbeatMonitor, 0,
          Constants.HEARTRATE );

      // Start looping for user interaction
      controller.interact();
    } catch ( IOException ioe ) {
      logger.error( "Controller failed to start. "+ioe.getMessage() );
      System.exit( 1 );
    }
  }

  @Override
  public String getHost() {
    return this.host;
  }

  @Override
  public int getPort() {
    return this.port;
  }

  @Override
  public void onEvent(Event event, TCPConnection connection) {
    switch ( event.getType() ) {

      case Protocol.CHUNK_SERVER_SENDS_REGISTRATION:
        registrationHelper( event, connection, true );
        break;

      case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION:
        registrationHelper( event, connection, false );
        break;

      case Protocol.CLIENT_STORE:
        storeChunk( event, connection );
        break;

      case Protocol.CLIENT_REQUESTS_FILE_DELETE:
        deleteFile( event, connection );
        break;

      case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT:
        heartbeatHelper( event );
        break;

      case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT:
        pokeHelper( event );
        break;

      case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION:
        corruptionHelper( event );
        break;

      case Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO:
        clientRead( event, connection );
        break;

      case Protocol.CLIENT_REQUESTS_FILE_LIST:
        fileListRequest( connection );
        break;

      case Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE:
        logger.debug( "ChunkServer acknowledges deletion of "+
                      (( GeneralMessage ) event).getMessage() );
        break;

      case Protocol.CLIENT_REQUESTS_SERVER_LIST:
        serverListRequest( connection );
        break;

      default:
        logger.debug( "Event couldn't be processed. "+event.getType() );
        break;
    }
  }

  /**
   * Respond to a request for the list of servers constituting the DFS.
   *
   * @param connection that produced the event
   */
  private synchronized void serverListRequest(TCPConnection connection) {
    StringBuilder sb = new StringBuilder();
    for ( ServerConnection server : information
                                        .getRegisteredServers()
                                        .values() ) {
      sb.append( server.toString() ).append( '\n' );
    }
    GeneralMessage message =
        new GeneralMessage( Protocol.CONTROLLER_SENDS_SERVER_LIST,
            sb.toString() );
    try {
      connection.getSender().sendData( message.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug(
          "Unable to send response to Client containing list of servers. "+
          ioe.getMessage() );
    }
  }

  /**
   * Respond to a request for the list of files stored on the DFS.
   *
   * @param connection that produced the event
   */
  private synchronized void fileListRequest(TCPConnection connection) {
    String[] fileList =
        information.getFileTable().keySet().toArray( new String[0] );
    if ( fileList.length == 0 ) {
      fileList = null;
    }
    ControllerSendsFileList response = new ControllerSendsFileList( fileList );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug(
          "Unable to send response to Client containing list of files. "+
          ioe.getMessage() );
    }
  }

  /**
   * Gather information about where a particular file is stored on the DFS, and
   * send those storage details back to the Client.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private synchronized void clientRead(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();
    // Get servers for all the file's chunks
    String[][] servers = null;
    if ( information.getFileTable().containsKey( filename ) ) {
      TreeMap<Integer, String[]> chunks =
          information.getFileTable().get( filename );
      servers = new String[chunks.size()][];
      int index = 0;
      for ( String[] chunkServer : chunks.values() ) {
        servers[index] = chunkServer;
        index++;
      }
    }
    ControllerSendsStorageList response =
        new ControllerSendsStorageList( filename, servers );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug(
          "Unable to send response to Client containing storage information "+
          "about '"+filename+"'. "+ioe.getMessage() );
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
   * the server that produced this corruption event.
   *
   * @param event message being handled
   */
  private synchronized void corruptionHelper(Event event) {
    ChunkServerReportsFileCorruption report =
        ( ChunkServerReportsFileCorruption ) event;

    String baseFilename =
        FilenameUtilities.getBaseFilename( report.getFilename() );
    int sequence = FilenameUtilities.getSequence( report.getFilename() );

    String destination =
        information.getChunkServerAddress( report.getIdentifier() );
    String[] servers = information.getServers( baseFilename, sequence );

    // If no servers hold this chunk, there is nothing to be done
    if ( servers == null ) {
      logger.debug(
          "The Controller doesn't have an entry in its fileTable for '"+
          report.getFilename()+"', so it cannot be repaired." );
      return;
    } else if ( destination == null ) {
      logger.debug(
          "Could not fetch the destination address of the server needing the "+
          "repair." );
      return;
    }

    // Construct the appropriate repair message and find out who to send the
    // message to first
    ForwardInformation forwardInformation =
        ControllerInformation.constructRepairMessage( report.getFilename(),
            servers, destination, report.getSlices() );

    if ( forwardInformation.firstHop() != null ) {
      try {
        information
            .getConnection( forwardInformation.firstHop() )
            .getConnection()
            .getSender()
            .sendData( forwardInformation.repairMessage().getBytes() );
      } catch ( IOException ioe ) {
        logger.debug( "Failed to send repair message to its first hop." );
      }
    }
  }

  /**
   * Update ServerConnection to reflect the fact that it responded to a poke
   * from the Controller.
   *
   * @param event message being handled
   */
  private void pokeHelper(Event event) {
    ChunkServerRespondsToHeartbeat response =
        ( ChunkServerRespondsToHeartbeat ) event;
    ServerConnection connection =
        information.getConnection( response.getIdentifier() );
    if ( connection == null ) {
      logger.debug( "There is no registered ChunkServer with an identifier of "+
                    response.getIdentifier()+"." );
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
  private void heartbeatHelper(Event event) {
    ChunkServerSendsHeartbeat heartbeat = ( ChunkServerSendsHeartbeat ) event;
    ServerConnection connection =
        information.getConnection( heartbeat.getIdentifier() );
    if ( connection == null ) {
      logger.debug( "There is no registered ChunkServer with an identifier of "+
                    heartbeat.getIdentifier()+"." );
      return;
    }
    connection
        .getHeartbeatInfo()
        .update( heartbeat.getBeatType(), heartbeat.getFreeSpace(),
            heartbeat.getTotalChunks(), heartbeat.getFiles() );
  }

  /**
   * Handles requests to delete a file at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private synchronized void deleteFile(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();

    // Delete file from the fileTable, and send delete messages to servers too
    information.deleteFileFromDFS( filename );

    // Send client an acknowledgement
    GeneralMessage response =
        new GeneralMessage( Protocol.CONTROLLER_APPROVES_FILE_DELETE,
            filename );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug( "Unable to acknowledge Client's request to delete file. "+
                    ioe.getMessage() );
    }
  }

  /**
   * Handles requests to store a chunk at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private synchronized void storeChunk(Event event, TCPConnection connection) {
    ClientStore request = ( ClientStore ) event;

    // Check if we've already allocated that particular filename/sequence combo
    String[] servers =
        information.getServers( request.getFilename(), request.getSequence() );

    // If it wasn't previously allocated, try to allocate it
    if ( servers == null ) {
      servers = information.allocateServers( request.getFilename(),
          request.getSequence() );
      if ( servers != null ) {
        for ( String address : servers ) {
          information
              .getConnection( address )
              .addChunk( request.getFilename(), request.getSequence() );
        }
      }
    }

    // Choose which response to send
    Event response = servers == null ? new GeneralMessage(
        Protocol.CONTROLLER_DENIES_STORAGE_REQUEST, request.getFilename() ) :
                         new ControllerReservesServers( request.getFilename(),
                             request.getSequence(), servers );
    // Respond to the Client
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug( "Unable to respond to Client's request to store chunk. "+
                    ioe.getMessage() );
    }
  }

  /**
   * Handles registration requests at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   * @param type true = register, false = deregister
   */
  private synchronized void registrationHelper(Event event,
      TCPConnection connection, boolean type) {
    GeneralMessage request = ( GeneralMessage ) event;
    if ( type ) { // attempt to register
      String address = request.getMessage();
      int registrationStatus = information.register( address, connection );

      // Respond to ChunkServer
      GeneralMessage message = new GeneralMessage(
          Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS,
          String.valueOf( registrationStatus ) );
      try {
        connection.getSender().sendData( message.getBytes() );
        if ( registrationStatus != -1 ) {
          // give new registrant the files it has been allocated, if any
          information.refreshServerFiles( registrationStatus );
        }
      } catch ( IOException ioe ) {
        logger.debug( "Failed to notify ChunkServer of registration status. "+
                      "Deregistering. "+ioe.getMessage() );
        if ( registrationStatus != -1 ) {
          information.deregister( registrationStatus );
        }
      }
    } else { // deregister
      information.deregister( Integer.parseInt( request.getMessage() ) );
    }
  }

  /**
   * A public deregister method which just takes the identifier of the server to
   * be deregistered. Is simple, but needed for the HeartbeatMonitor because it
   * has the proper synchronization.
   *
   * @param identifier of the server to be deregistered
   */
  public synchronized void deregister(int identifier) {
    information.deregister( identifier );
  }

  /**
   * Loops for user input to the Controller.
   */
  private void interact() {
    System.out.println(
        "Enter a command or use 'help' to print a list of commands." );
    Scanner scanner = new Scanner( System.in );
    while ( true ) {
      String command = scanner.nextLine();
      String[] splitCommand = command.split( "\\s+" );
      switch ( splitCommand[0].toLowerCase() ) {

        case "s":
        case "servers":
          listRegisteredChunkServers();
          break;

        case "f":
        case "files":
          listAllocatedFiles();
          break;

        case "h":
        case "help":
          showHelp();
          break;

        default:
          System.err.println( "Unrecognized command. Use 'help' command." );
          break;
      }
    }
  }

  /**
   * Prints a list of files allocated by the Controller.
   */
  private synchronized void listAllocatedFiles() {
    String[] fileList =
        information.getFileTable().keySet().toArray( new String[0] );
    for ( String filename : fileList ) {
      System.out.printf( "%3s%s%n", "", filename );
    }
  }

  /**
   * Prints a list of registered ChunkServers.
   */
  private synchronized void listRegisteredChunkServers() {
    for ( ServerConnection connection : information
                                            .getRegisteredServers()
                                            .values() ) {
      System.out.printf( "%3s%s%n", "", connection.toString() );
    }
  }

  /**
   * Prints a list of commands available to the user.
   */
  private void showHelp() {
    System.out.printf( "%3s%-9s : %s%n", "", "s[ervers]",
        "print the addresses of all registered ChunkServers" );
    System.out.printf( "%3s%-9s : %s%n", "", "f[iles]",
        "print the names of all files allocated for storage" );
    System.out.printf( "%3s%-9s : %s%n", "", "h[elp]",
        "print a list of valid commands" );
  }
}