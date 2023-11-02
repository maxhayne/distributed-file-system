package cs555.overlay.node;

import cs555.overlay.transport.ControllerInformation;
import cs555.overlay.transport.ServerConnection;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.Constants;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
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

      System.out.println( "Controller's ServerThread has started at ["+host+":"+
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
      System.err.println( "Controller failed to start. "+ioe.getMessage() );
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

      // Might not need, would be received by ChunkServer processing a
      // SendsFileForStorage request
      case Protocol.CHUNK_SERVER_NO_STORE_FILE:
        missingFileHelper( event );
        break;

      // Might not need this anymore, fix will be picked up in heartbeat?
      case Protocol.CHUNK_SERVER_REPORTS_FILE_FIX:
        //markFileFixed( event );
        break;

      // Better to combine this case and next into one?
      case Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO:
        clientRead( event, connection );
        break;

      case Protocol.CLIENT_REQUESTS_FILE_SIZE:
        fileSizeRequest( event, connection );
        break;

      case Protocol.CLIENT_REQUESTS_FILE_LIST:
        fileListRequest( connection );
        break;

      default:
        System.err.println( "Event couldn't be processed. "+event.getType() );
        break;
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
      System.err.println( "fileListRequest: Unable to send response"+
                          " to Client containing list of files." );
    }
  }

  // Might not be needed anymore...

  /**
   * Respond to a request for the file size of a particular file stored on the
   * DFS (how many chunks it contains).
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private synchronized void fileSizeRequest(Event event,
      TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();

    TreeMap<Integer, String[]> sequences =
        information.getFileTable().get( filename );

    int totalChunks = sequences == null ? 0 : sequences.size();

    ControllerReportsFileSize response =
        new ControllerReportsFileSize( filename, totalChunks );

    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "fileSizeRequest: Unable to send response"+
                          " to Client with size of '"+filename+"'." );
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
      for ( String[] chunkServers : chunks.values() ) {
        servers[index] = chunkServers;
        index++;
      }
    }

    ControllerSendsStorageList response =
        new ControllerSendsStorageList( filename, servers );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "clientRead: Unable to send response to Client"+
                          " containing storage information about "+filename+
                          "." );
    }
  }

  //  /**
  //   * Mark the chunk/shard healthy (not corrupt) in the reportedState
  //   * DistributedFileCache. This action will allow the Controller to tell
  //   future
  //   * Clients or ChunkServers that this particular file at this particular
  //   server
  //   * is available to be requested.
  //   *
  //   * @param event message being handled
  //   */
  //  private synchronized void markFileFixed(Event event) {
  //    ChunkServerReportsFileFix report = ( ChunkServerReportsFileFix ) event;
  //
  //    // Mark the specified chunk/shard as healthy, so we can use this
  //    // ChunkServer as a source for future file requests.
  //    String baseFilename =
  //        FilenameUtilities.getBaseFilename( report.getFilename() );
  //    int sequence = FilenameUtilities.getSequence( report.getFilename() );
  //    if ( FileSynchronizer.checkChunkFilename( report.filename ) ) { // chunk
  //      connectionCache.getReportedState()
  //                     .markChunkHealthy( baseFilename, sequence,
  //                         report.getIdentifier() ); // Mark healthy
  //    } else if ( FileSynchronizer.checkShardFilename(
  //        report.getFilename() ) ) { // shard
  //      int fragment = FilenameUtilities.getFragment( report.getFilename() );
  //      connectionCache.getReportedState()
  //                     .markShardHealthy( baseFilename, sequence, fragment,
  //                         report.getIdentifier() ); // Mark healthy
  //    } else {
  //      System.err.println( "markFileFixed: '"+report.getFilename()+"' is
  //      not"+
  //                          " a valid name for either a chunk or a shard"+"
  //                          ." );
  //    }
  //  }

  // This isn't actually used at the moment -- the ChunkServer will never
  // send this message.

  /**
   * Change the address in the String[] of servers in the fileTable for this
   * particular filename and sequence number to null.
   *
   * @param event message being handled
   */
  private synchronized void missingFileHelper(Event event) {
    ChunkServerNoStoreFile message = ( ChunkServerNoStoreFile ) event;
    // We need to remove the Chunk with those properties from the
    // idealState, and find a new server that can store the file.

    // Remove missing Chunk from idealState
    String baseFilename =
        FilenameUtilities.getBaseFilename( message.getFilename() );
    int identifier =
        information.getChunkServerIdentifier( message.getAddress() );
    int sequence = FilenameUtilities.getSequence( message.getFilename() );
    if ( identifier != -1 ) {
      information.removeServer( baseFilename, sequence, message.getAddress() );
    }
    // Just remove the file for now. Can try to repair the file system
    // during heartbeats for chunks that aren't replicated 3 times.
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
      System.err.println( "corruptionHelper: The Controller doesn't have an "+
                          "entry in its fileTable for '"+report.getFilename()+
                          "', so it cannot be repaired." );
      return;
    } else if ( destination == null ) {
      System.err.println( "corruptionHelper: Could not fetch the destination "+
                          "address of the server needing the repair." );
      return;
    }

    Event repairMessage;
    String sendTo;
    if ( ApplicationProperties.storageType.equals( "erasure" ) ) {
      RepairShard repairShard =
          new RepairShard( report.getFilename(), destination, servers );
      sendTo = repairShard.getAddress();
      repairMessage = repairShard;
    } else {
      RepairChunk repairChunk =
          new RepairChunk( report.getFilename(), destination,
              report.getSlices(), servers );
      sendTo = repairChunk.getAddress();
      repairMessage = repairChunk;
    }

    try {
      information.getConnection( sendTo )
                 .getConnection()
                 .getSender()
                 .sendData( repairMessage.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "corruptionHelper: Failed to send repair message to"+
                          " its first hop." );
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
      System.err.println( "pokeHelper: there is no registered ChunkServer"+
                          " with an identifier of "+response.getIdentifier()+
                          "." );
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
    // How should the updating of the heartbeat information actually
    // happen? In the same way as before, or does a new function need
    // to be written to update the heartbeat information all in one go?
    // It should be done with one function call.
    ServerConnection connection =
        information.getConnection( heartbeat.getIdentifier() );
    if ( connection == null ) {
      System.err.println( "heartbeatHelper: there is no registered ChunkServer"+
                          "with an identifier of "+heartbeat.getIdentifier()+
                          "." );
      return;
    }
    connection.getHeartbeatInfo()
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

    // Delete the file from the fileTable
    information.deleteFile( filename );

    // Delete the file from the 'storedChunks' map of all servers in
    // registeredServers
    Map<Integer, ServerConnection> registeredServers =
        information.getRegisteredServers();
    for ( ServerConnection server : registeredServers.values() ) {
      server.deleteFile( filename );
    }

    // Send delete request to all registered ChunkServers
    GeneralMessage deleteRequest =
        new GeneralMessage( Protocol.CONTROLLER_REQUESTS_FILE_DELETE,
            filename );
    try {
      information.broadcast( deleteRequest.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "deleteFile: Error while sending file delete request to all "+
          "ChunkServers. "+ioe.getMessage() );
    }

    // Send client an acknowledgement
    GeneralMessage response =
        new GeneralMessage( Protocol.CONTROLLER_APPROVES_FILE_DELETE );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "deleteFile: Unable to acknowledge Client's request to "+"delete "+
          "file. "+ioe.getMessage() );
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
          information.getConnection( address )
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
      System.err.println(
          "Unable to respond to Client's request to store chunk. "+
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
      } catch ( IOException ioe ) {
        System.err.println(
            "Failed to notify ChunkServer of registration status. "+
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
    System.out.println( "deregister called "+identifier );
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

        case "servers":
          listRegisteredChunkServers();
          break;

        case "files":
          listAllocatedFiles();
          break;

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
    String[] servers = information.getAllServerAddresses();
    for ( String server : servers ) {
      System.out.printf( "%3s%s%n", "", server );
    }
  }

  /**
   * Prints a list of commands available to the user.
   */
  private void showHelp() {
    System.out.printf( "%3s%-8s : %s%n", "", "servers",
        "print the addresses of all registered ChunkServers" );
    System.out.printf( "%3s%-8s : %s%n", "", "files",
        "print the names of all files earmarked for storage" );
    System.out.printf( "%3s%-8s : %s%n", "", "help",
        "print a list of valid commands" );
  }
}