package cs555.overlay.node;

import cs555.overlay.transport.ServerConnection;
import cs555.overlay.transport.ServerConnectionCache;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.*;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;

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
  private final ServerConnectionCache connectionCache;

  public Controller(String host, int port) {
    this.host = host;
    this.port = port;
    this.connectionCache =
        new ServerConnectionCache( new DistributedFileCache(),
            new DistributedFileCache() );
  }

  /**
   * Entry point for the Controller. Creates a ServerSocket at the port
   * specified in the 'application.properties' file, creates a
   * ServerConnectionCache (which starts the HeartbeatMonitor), starts
   * looping for user commands.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    try ( ServerSocket serverSocket = new ServerSocket(
        ApplicationProperties.controllerPort ) ) {

      String host = serverSocket.getInetAddress().getHostAddress();
      Controller controller =
          new Controller( host, ApplicationProperties.controllerPort );

      (new Thread( new TCPServerThread( controller, serverSocket ) )).start();

      System.out.println( "Controller's ServerThread has started at ["+host+":"+
                          ApplicationProperties.controllerPort+"]" );
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

      case Protocol.CLIENT_STORE_CHUNK:
      case Protocol.CLIENT_STORE_SHARDS:
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
        markFileFixed( event );
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
  private void fileListRequest(TCPConnection connection) {

    ControllerSendsFileList response = new ControllerSendsFileList(
        connectionCache.getIdealState().getFileList() );

    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "fileListRequest: Unable to send response"+
                          " to Client containing list of files." );
    }
  }

  /**
   * Respond to a request for the file size of a particular file stored on the
   * DFS (how many chunks it contains).
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private void fileSizeRequest(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();
    ControllerReportsFileSize response =
        new ControllerReportsFileSize( filename,
            connectionCache.getIdealState().getFileSize( filename ) );

    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "fileSizeRequest: Unable to send response"+
                          " to Client with size of '"+filename+"'." );
    }
  }

  /**
   * Gather information about where a particular file is stored on the DFS,
   * whether that is a chunk, or an entire file, and send those storage details
   * back to the Client.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private void clientRead(Event event, TCPConnection connection) {
    GeneralMessage request = ( GeneralMessage ) event;

    String baseFilename;
    int sequence;
    // Check if the filename refers to a chunk, shard, or neither
    if ( FileSynchronizer.checkChunkFilename(
        request.getMessage() ) ) { // chunk
      String[] split = request.getMessage().split( "_chunk" );
      baseFilename = split[0];
      sequence = Integer.parseInt( split[1] );
    } else if ( FileSynchronizer.checkShardFilename(
        request.getMessage() ) ) { // shard
      String[] split = request.getMessage().split( "_chunk" );
      baseFilename = split[0];
      split = split[1].split( "_shard" );
      sequence = Integer.parseInt( split[0] );
    } else {
      // Add in functionality for returning the storage information
      // about an entire file here...
      // For now, print an error message
      System.err.println( "clientRead: '"+request.getMessage()+
                          "' does not refer to a chunk or a shard." );
      return;
    }

    // Get storage information
    String storageInfo =
        connectionCache.getChunkStorageInfo( baseFilename, sequence );
    if ( storageInfo.equals( "|" ) ) {
      System.err.println( "clientRead: '"+request.getMessage()+
                          "' is stored on no ChunkServer." );
      return; // There are no ChunkServers storing either chunks or shards
    }

    // Create response message
    String[] split = storageInfo.split( "\\|", -1 );
    String[] replications = split[0].split( "," );
    String[] shards = split[1].split( "," );

    ControllerSendsStorageList response;
    if ( ApplicationProperties.storageType.equals( "replication" ) ) {
      response = new ControllerSendsStorageList( baseFilename, replications );
    } else {
      ArrayUtilities.replaceArrayItem( shards, "-1", null );
      response = new ControllerSendsStorageList( baseFilename, shards );
    }

    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "clientRead: Unable to send response to Client"+
                          " containing storage information about "+
                          request.getMessage()+"." );
    }
  }

  /**
   * Mark the chunk/shard healthy (not corrupt) in the reportedState
   * DistributedFileCache. This action will allow the Controller to tell future
   * Clients or ChunkServers that this particular file at this particular server
   * is available to be requested.
   *
   * @param event message being handled
   */
  private void markFileFixed(Event event) {
    ChunkServerReportsFileFix report = ( ChunkServerReportsFileFix ) event;

    // Mark the specified chunk/shard as healthy, so we can use this
    // ChunkServer as a source for future file requests.
    String baseFilename;
    int sequence;
    if ( FileSynchronizer.checkChunkFilename(
        report.filename ) ) { // chunk
      String[] split = report.filename.split( "_chunk" );
      baseFilename = split[0];
      sequence = Integer.parseInt( split[1] );
      connectionCache.getReportedState()
                     .markChunkHealthy( baseFilename, sequence,
                         report.identifier ); // Mark healthy
    } else if ( FileSynchronizer.checkShardFilename(
        report.filename ) ) { // shard
      String[] split = report.filename.split( "_chunk" );
      baseFilename = split[0];
      split = split[1].split( "_shard" );
      sequence = Integer.parseInt( split[0] );
      int fragment = Integer.parseInt( split[1] );
      connectionCache.getReportedState()
                     .markShardHealthy( baseFilename, sequence, fragment,
                         report.identifier ); // Mark healthy
    } else {
      System.err.println( "markFileFixed: '"+report.filename+"' is not"+
                          " a valid name for either a chunk or a shard"+"." );
    }
  }

  /**
   * Update the idealState DistributedFileCache to indicate that the filename
   * provided in the message is not stored there. Then, find a suitable
   * ChunkServer to store the missing replication or shard.
   *
   * @param event message being handled
   */
  private void missingFileHelper(Event event) {
    ChunkServerNoStoreFile message = ( ChunkServerNoStoreFile ) event;
    // We need to remove the Chunk with those properties from the
    // idealState, and find a new server that can store the file.

    // Remove missing Chunk from idealState
    int identifier =
        connectionCache.getChunkServerIdentifier( message.address );
    int sequence = Integer.parseInt( message.filename.split( "_chunk" )[1] );
    if ( identifier != -1 ) {
      connectionCache.getIdealState()
                     .removeChunk( new Chunk( message.filename, sequence, 0, 0,
                         identifier, false ) );
    }
    // Just remove the file for now. Can try to repair the file system
    // during heartbeats for chunks that aren't replicated 3 times.
  }

  /**
   * Changes the reportedState DistributedFileCache to reflect the fact that a
   * file at the sender (a ChunkServer) is corrupt. Then, if possible, starts a
   * relay chain that visits all ChunkServers that have replica chunks (or
   * shards) for that particular file, whose destination is the ChunkServer with
   * the corrupt file.
   *
   * @param event message being handled
   */
  private void corruptionHelper(Event event) {
    ChunkServerReportsFileCorruption report =
        ( ChunkServerReportsFileCorruption ) event;

    // Mark the specified chunk/shard as corrupt, so that we don't tell
    // a Client to look there for a copy.
    String baseFilename;
    int sequence;
    if ( FileSynchronizer.checkChunkFilename( report.filename ) ) {
      String[] split = report.filename.split( "_chunk" );
      baseFilename = split[0];
      sequence = Integer.parseInt( split[1] );
      connectionCache.getReportedState()
                     .markChunkCorrupt( baseFilename, sequence,
                         report.identifier ); // Mark the chunk corrupt
    } else if ( FileSynchronizer.checkShardFilename(
        report.filename ) ) {
      String[] split = report.filename.split( "_chunk" );
      baseFilename = split[0];
      split = split[1].split( "_shard" );
      sequence = Integer.parseInt( split[0] );
      int fragment = Integer.parseInt( split[1] );
      connectionCache.getReportedState()
                     .markShardCorrupt( baseFilename, sequence, fragment,
                         report.identifier ); // Mark the shard corrupt
    } else {
      System.err.println( "corruptionHelper: '"+report.filename+"' is not"+
                          " a valid name for either a chunk or a shard"+"." );
      return;
    }

    // Get list of ChunkServer host:port combos where chunk replacement
    // or shards are available.
    String info = connectionCache.getChunkStorageInfo( baseFilename, sequence );
    System.out.println( info );
    if ( info.equals( "|" ) ) {
      System.err.println( "corruptionHelper: No servers could be found which"+
                          " could help repair '"+report.filename+
                          "' on ChunkServer "+report.identifier+"." );
      return;
    }

    // Split servers into servers holding chunks and servers holding shards
    String[] split = info.split( "\\|", -1 );
    String[] replicationServers = split[0].split( "," );
    String[] shardServers = split[1].split( "," );

    // Send a RepairChunk or RepairShard message to a ChunkServer that
    // can help
    if ( FileSynchronizer.checkShardFilename( report.filename ) ) {
      if ( shardServers.length == Constants.TOTAL_SHARDS ) {
        int count =
            Collections.frequency( Arrays.asList( shardServers ), "-1" );
        if ( count >= Constants.DATA_SHARDS ) {
          ArrayUtilities.replaceArrayItem( shardServers, "-1", null );
          RepairShard repairMessage = new RepairShard( report.filename,
              connectionCache.getChunkServerAddress( report.identifier ),
              shardServers );
          if ( !repairMessage.getDestination().isEmpty() ) {
            try {
              connectionCache.getConnection( repairMessage.getAddress() )
                             .getConnection()
                             .getSender()
                             .sendData( repairMessage.getBytes() );
            } catch ( IOException ioe ) {
              System.err.println(
                  "corruptionHelper: Failed to send RepairShard to "+
                  "ChunkServer." );
            }
          }

        }
      }
    } else {
      if ( !replicationServers[0].isEmpty() ) {
        String serverAddress =
            connectionCache.getChunkServerAddress( report.identifier );
        if ( !serverAddress.isEmpty() ) {
          replicationServers =
              ArrayUtilities.removeFromArray( replicationServers,
                  serverAddress );
          if ( replicationServers.length != 0 ) {
            RepairChunk repairMessage =
                new RepairChunk( report.filename, serverAddress, report.slices,
                    replicationServers );
            try {
              connectionCache.getConnection( repairMessage.getAddress() )
                             .getConnection()
                             .getSender()
                             .sendData( repairMessage.getBytes() );
            } catch ( IOException ioe ) {
              System.err.println( "corruptionHelper: Failed to send "+
                                  "RepairChunk to ChunkServer." );
            }
          }
        }
      }
    }
  }

  /**
   * Update ServerConnection to reflect the fact that it responded to a
   * poke from the Controller.
   *
   * @param event message being handled
   */
  private void pokeHelper(Event event) {
    ChunkServerRespondsToHeartbeat response =
        ( ChunkServerRespondsToHeartbeat ) event;
    ServerConnection connection =
        connectionCache.getConnection( response.identifier );
    if ( connection == null ) {
      System.err.println( "pokeHelper: there is no registered ChunkServer"+
                          " with an identifier of "+response.identifier+"." );
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
        connectionCache.getConnection( heartbeat.identifier );
    if ( connection == null ) {
      System.err.println( "heartbeatHelper: there is no registered ChunkServer"+
                          "with an identifier of "+heartbeat.identifier+"." );
      return;
    }
    connection.getHeartbeatInfo()
              .update( heartbeat.type, heartbeat.freeSpace,
                  heartbeat.totalChunks, heartbeat.files );
  }

  /**
   * Handles requests to delete a file at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private void deleteFile(Event event, TCPConnection connection) {
    GeneralMessage request = ( GeneralMessage ) event;

    // Remove file from DistributedFileCache
    connectionCache.getIdealState().removeFile( request.getMessage() );

    // Send delete request to all registered ChunkServers
    GeneralMessage deleteRequest =
        new GeneralMessage( Protocol.CONTROLLER_REQUESTS_FILE_DELETE,
            request.getMessage() );
    try {
      connectionCache.broadcast( deleteRequest.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "Error while sending file delete request to all ChunkServers. "+
          ioe.getMessage() );
    }

    // Send client an acknowledgement
    GeneralMessage response =
        new GeneralMessage( Protocol.CONTROLLER_APPROVES_FILE_DELETE );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "Unable to acknowledge Client's request to "+"delete file. "+
          ioe.getMessage() );
    }
  }

  /**
   * Handles requests to store a chunk at the Controller.
   *
   * @param event message being handled
   * @param connection that produced the event
   */
  private void storeChunk(Event event, TCPConnection connection) {
    ClientStore request = ( ClientStore ) event;

    // Try to reserve servers to store chunk in DistributedFileSystem
    String reserved = request.getType() == Protocol.CLIENT_STORE_CHUNK ?
                          connectionCache.availableChunkServers(
                              request.getFilename(), request.getSequence() ) :
                          connectionCache.availableShardServers(
                              request.getFilename(), request.getSequence() );

    String[] servers = reserved.split( "," ); // split into array

    // Choose which response to send
    Event response = servers[0].isEmpty() ? new GeneralMessage(
        Protocol.CONTROLLER_DENIES_STORAGE_REQUEST, request.getFilename() ) :
                         new ControllerReservesServers( request.getFilename(),
                             request.getSequence(), servers );

    // Respond to Client
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
  private void registrationHelper(Event event, TCPConnection connection,
      boolean type) {
    GeneralMessage request = ( GeneralMessage ) event;
    if ( type ) { // attempt to register
      String address = request.getMessage();
      int registrationStatus = connectionCache.register( address, connection );

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
          connectionCache.deregister( registrationStatus );
        }
      }
    } else { // deregister
      connectionCache.deregister( Integer.parseInt( request.getMessage() ) );
    }
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
  private void listAllocatedFiles() {
    String[] fileList = connectionCache.getIdealState().getFileList();
    if ( fileList != null ) {
      for ( String filename : fileList ) {
        System.out.printf( "%3s%s%n", "", filename );
      }
    }
  }

  /**
   * Prints a list of registered ChunkServers.
   */
  private void listRegisteredChunkServers() {
    String[] servers = connectionCache.getAllServerAddresses();
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