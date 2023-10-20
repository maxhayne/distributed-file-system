package cs555.overlay.node;

import cs555.overlay.files.ChunkReader;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.*;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkServer implements Node {

  private final String host;
  private final int port;
  private final TCPConnectionCache connectionCache;
  // variables that are set upon successful registration with Controller
  private final AtomicBoolean isRegistered;
  private TCPConnection controllerConnection;
  private int identifier;
  private FileDistributionService fileService;
  private Timer heartbeatTimer;

  public ChunkServer(String host, int port) {
    this.host = host;
    this.port = port;
    this.connectionCache = new TCPConnectionCache();
    this.isRegistered = new AtomicBoolean( false );
  }

  public static void main(String[] args) throws Exception {
    // Start the TCPServerThread, so that when we try to register with
    // the Controller, we can guarantee it is already running.

    // Taking the storage directory was fine for testing purposes, but
    // let's convert ChunkServers to store their files in the /tmp
    // directory.

    // If an argument is provided by the user, interpret it as a custom
    // port for the TCPServerThread to run on, and try to use it. Will
    // throw an Exception if the argument is not an integer, but
    int serverPort = args.length > 0 ? Integer.parseInt( args[0] ) : 0;

    try ( ServerSocket serverSocket = new ServerSocket( serverPort );
          Socket controllerSocket = new Socket(
              ApplicationProperties.controllerHost,
              ApplicationProperties.controllerPort ) ) {

      String host = InetAddress.getLocalHost().getHostAddress();
      ChunkServer chunkServer =
          new ChunkServer( host, serverSocket.getLocalPort() );

      // Start the TCPServerThread
      (new Thread( new TCPServerThread( chunkServer, serverSocket ) )).start();

      System.out.println(
          "TCPServerThread is started and available at ["+chunkServer.getHost()+
          ":"+chunkServer.getPort()+"]" );

      // Establish socket connection with controller, send a registration
      // request, and start the TCPReceiverThread
      chunkServer.controllerConnection =
          new TCPConnection( chunkServer, controllerSocket );
      chunkServer.sendGeneralMessage( Protocol.CHUNK_SERVER_SENDS_REGISTRATION,
          host+":"+serverSocket.getLocalPort(),
          chunkServer.controllerConnection );
      System.out.println(
          "A registration request has been sent to the Controller." );
      chunkServer.controllerConnection.start();

      // Loop for user interaction
      chunkServer.interact();
    } catch ( IOException ioe ) {
      System.err.println( "ChunkServer failed to start. "+ioe.getMessage() );
      System.exit( 1 );
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
    // If the event being processed wasn't sent by the Controller, check
    // to make sure that the ChunkServer is registered. If not, don't
    // process the Event.
    if ( connection != controllerConnection && !isRegistered.get() ) {
      System.err.println(
          "onEvent: Event wasn't be processed because the ChunkServer "+
          "isn't registered. "+event.getType() );
      return;
    }

    switch ( event.getType() ) {
      case Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS:
        registrationInterpreter( event );
        break;

      case Protocol.CONTROLLER_REQUESTS_FILE_DELETE:
        deleteRequestHelper( event, connection );
        break;

      case Protocol.SENDS_FILE_FOR_STORAGE:
        storeAndRelay( event, connection );
        break;

      case Protocol.REQUESTS_CHUNK:
        serveChunk( event, connection );
        break;

      case Protocol.REQUESTS_SHARD:
        serveShard( event, connection );
        break;

      case Protocol.CONTROLLER_SENDS_HEARTBEAT:
        acknowledgeHeartbeat( connection );
        break;

      case Protocol.REPAIR_CHUNK:
        repairChunkHelper( event );
        break;

      case Protocol.CHUNK_SERVER_SERVES_FILE:
        // This will be for when the ChunkServer receives a
        // replacement file for one of its files. It can then modify
        // the version number and timestamp in the metadata, and
        // recompute the first hash, and store the file on disk.
        break;

      default:
        System.err.println( "Event couldn't be processed. "+event.getType() );
        break;
    }
  }

  /**
   * Checks to see if this ChunkServer is storing the relevant file. If it is,
   * it tries to retrieve the relevant slices which need repairing from that
   * file. If all slices needed for the repair are present, attaches the slices
   * to the message and sends the message to the ChunkServer needing the repair.
   * If not all slices were found and there is another ChunkServer that might
   * store the needed slices, forwards the message to that server.
   *
   * @param event message being processed
   */
  private void repairChunkHelper(Event event) {
    RepairChunk repairMessage = ( RepairChunk ) event;
    ChunkReader chunkReader = new ChunkReader( repairMessage.getFilename() );

  }

  /**
   * Calls a helper function, which returns true if the slices could be served,
   * and false if they couldn't. If false is returned, a message is sent to the
   * requester of the slices denying their request.
   *
   * @param event message request for slices
   * @param connection that sent the message
   */
  private void serveSlices(Event event, TCPConnection connection) {
    RequestsSlices request = ( RequestsSlices ) event;
    boolean success = serveSlicesHelper( request, connection );
    if ( !success ) {
      System.out.println(
          "serveSlices: Unable to serve slices of '"+request.filename+
          "' to requester." );
      try {
        sendGeneralMessage( Protocol.CHUNK_SERVER_DENIES_REQUEST,
            request.filename, connection );
      } catch ( IOException ioe ) {
        System.out.println( "serveSlices: Unable to send denial of request. "+
                            ioe.getMessage() );
      }
    }
  }

  /**
   * Since there are a few points of failure in being able to serve chunk
   * slices, this function was created to return false upon any of those
   * failures. If it doesn't fail, this function serves the slices it can to the
   * requester, and returns true.
   *
   * @param request message request for slices
   * @param connection that sent the message
   * @return true if served, false if not
   */
  private boolean serveSlicesHelper(RequestsSlices request,
      TCPConnection connection) {

    // Check if filename is properly formatted for a chunk, and try to
    // read file if it is
    byte[] fileBytes;
    if ( FileDistributionService.checkChunkFilename( request.filename ) ) {
      fileBytes = fileService.readBytesFromFile( request.filename );
    } else {
      return false;
    }

    // If read failed, return false
    if ( fileBytes == null ) {
      return false;
    }

    // Create vector of slices that are both requested, and healthy,
    // called servableSlices
    ArrayList<Integer> corruptSlices =
        FileDistributionService.checkChunkForCorruption( fileBytes );
    ArrayList<Integer> servableSlices = new ArrayList<Integer>();
    for ( int i = 0; i < request.slices.length; ++i ) {
      if ( !corruptSlices.contains( request.slices[i] ) ) {
        servableSlices.add( request.slices[i] );
      }
    }

    // If there are corrupt slices, make sure the fileService tries to
    // repair them
    int[] slicesToRepair = ArrayUtilities.arrayListToArray( corruptSlices );
    ChunkServerReportsFileCorruption report =
        new ChunkServerReportsFileCorruption( identifier, request.filename,
            slicesToRepair );
    try {
      controllerConnection.getSender().sendData( report.getBytes() );
    } catch ( IOException ioe ) {
      System.out.println( "serveFileToChunkServerHelper: Couldn't notify "+
                          "Controller of corruption. "+ioe.getMessage() );
    }

    // If no slices can be served, deny the request for slices
    if ( servableSlices.isEmpty() ) {
      return false;
    } else { // Serve slices that are healthy
      int[] cleanSlices = ArrayUtilities.arrayListToArray( servableSlices );
      // Create slices array we can serve
      byte[][] slicesToServe = fileService.getSlices( fileBytes, cleanSlices );
      ChunkServerServesSlices serveSlices =
          new ChunkServerServesSlices( request.filename, cleanSlices,
              slicesToServe );
      try {
        connection.getSender().sendData( serveSlices.getBytes() );
      } catch ( IOException ioe ) {
        System.out.println(
            "serveSlicesHelper: Unable to serve "+"slices to requester. "+
            ioe.getMessage() );
      }
    }
    return true;
  }

  /**
   * Respond to Controller's heartbeat message. In the Controller, these
   * messages that it sends to a ChunkServer are called 'pokes', and the
   * responses it receives are called 'pokeReplies', and a count of each is kept
   * in the ChunkServerConnection with this ChunkServer, and if the discrepancy
   * between pokes and pokeReplies becomes too great, the ChunkServer is
   * automatically deregistered.
   *
   * @param connection
   */
  private void acknowledgeHeartbeat(TCPConnection connection) {
    if ( connection == controllerConnection ) {
      ChunkServerRespondsToHeartbeat ack =
          new ChunkServerRespondsToHeartbeat( identifier );
      try {
        connection.getSender().sendData( ack.getBytes() );
      } catch ( IOException ioe ) {
        System.out.println( "acknowledgeHeartbeat: Unable to send "+
                            "response to Controller's heartbeat. "+
                            ioe.getMessage() );
      }
    } else {
      System.out.println( "acknowledgeHeartbeat: Received a heartbeat, "+
                          "but it wasn't from the Controller. "+
                          "Ignoring it." );
    }
  }

  /**
   * Calls a helper function that makes an attempt to serve a shard to the
   * connection that requested it. If the helper function can't do that, for any
   * reason, it returns false, and this function sends a denial of the request
   * to the connection that sent it.
   *
   * @param event
   * @param connection
   */
  private void serveShard(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();
    boolean success = serveShardHelper( filename, connection );
    if ( !success ) {
      System.out.println(
          "serveShard: Unable to serve '"+filename+"' to requester." );
      try {
        sendGeneralMessage( Protocol.CHUNK_SERVER_DENIES_REQUEST, filename,
            connection );
      } catch ( IOException ioe ) {
        System.err.println(
            "serveShard: Unable to send denial of request. "+ioe.getMessage() );
      }
    }
  }

  /**
   * Reads the file with the filename given in the message. If the data can be
   * read without error, and the shard isn't corrupt, it serves the shard's data
   * to the requester and returns true. If the file cannot be read or the shard
   * is corrupt, it denies the request, adds an event to the fileService queue,
   * and returns false.
   *
   * @param filename
   * @param connection
   * @return true if the shard was served, false if it wasn't
   */
  private boolean serveShardHelper(String filename, TCPConnection connection) {

    // Attempt to read the file
    byte[] fileBytes;
    if ( FileDistributionService.checkShardFilename( filename ) ) {
      fileBytes = fileService.readBytesFromFile( filename );
    } else { // neither chunk nor shard, deny request
      System.err.println( "serveShardHelper: '"+filename+"' does not "+
                          "have a proper name for a shard." );
      return false;
    }

    // An attempt has been made to read the file, was it successful?
    if ( fileBytes == null ) { // not successful
      return false;
    }

    // Check if shard is corrupt
    boolean corrupt =
        FileDistributionService.checkShardForCorruption( fileBytes );
    if ( corrupt ) { // Need to report the file corruption and deny the
      // request.
      ChunkServerReportsFileCorruption report =
          new ChunkServerReportsFileCorruption( identifier, filename, null );
      try {
        controllerConnection.getSender().sendData( report.getBytes() );
      } catch ( IOException ioe ) {
        System.out.println( "serveFileToChunkServerHelper: Couldn't notify "+
                            "Controller of corruption. "+ioe.getMessage() );
      }
      return false;
    }

    // The shard wasn't corrupted
    byte[] cleanBytes = FileDistributionService.getDataFromShard(
        FileDistributionService.removeHashFromShard( fileBytes ) );
    ChunkServerServesFile response =
        new ChunkServerServesFile( filename, cleanBytes );
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "serveShardHelper: Couldn't serve '"+filename+"' to requester. "+
          ioe.getMessage() );
    }
    return true;
  }

  private void serveChunk(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();
    boolean success = serveChunkHelper( filename, connection );
    if ( !success ) {
      System.out.println(
          "serveChunk: Unable to serve '"+filename+"' to requester." );
      try {
        sendGeneralMessage( Protocol.CHUNK_SERVER_DENIES_REQUEST, filename,
            connection );
      } catch ( IOException ioe ) {
        System.err.println(
            "serveChunk: Unable to send denial of request. "+ioe.getMessage() );
      }
    }
  }

  /**
   * Deals with a request for a chunk (which would come from another ChunkServer
   * or the Client). Attempts to read the file with the filename sent in the
   * message. Many things must go right to succeed. First, the filename must be
   * that of a properly formatted chunk, next, the file must exist, the read
   * must succeed, and the chunk mustn't be corrupted. If any of those things
   * don't happen, the read fails, false is returned and a failure response is
   * sent to the requester, and, if the failure happened because the file is
   * corrupt, the fileService will deal with the corruption event.
   *
   * @param filename of chunk to serve
   * @param connection that requested the chunk
   */
  private boolean serveChunkHelper(String filename, TCPConnection connection) {

    byte[] fileBytes;
    if ( FileDistributionService.checkChunkFilename( filename ) ) {
      fileBytes = fileService.readBytesFromFile( filename );
    } else { // neither chunk nor shard, deny request
      System.err.println( "serveChunk: '"+filename+"' does not "+
                          "have a proper name for a chunk." );
      return false;
    }

    // An attempt has been made to read the file, was it successful?
    if ( fileBytes == null ) { // not successful
      return false;
    }

    // Check chunk for errors, create corruptSlices int array filled with
    // indices of corrupt slices
    ArrayList<Integer> errors =
        FileDistributionService.checkChunkForCorruption( fileBytes );
    int[] corruptSlices = null;
    if ( !errors.isEmpty() ) {
      corruptSlices = ArrayUtilities.arrayListToArray( errors );
    }

    // If there are corrupt slices, give event to fileService to deal with
    // and deny request
    if ( corruptSlices != null ) {
      ChunkServerReportsFileCorruption report =
          new ChunkServerReportsFileCorruption( identifier, filename,
              corruptSlices );
      try {
        controllerConnection.getSender().sendData( report.getBytes() );
      } catch ( IOException ioe ) {
        System.out.println( "serveFileToChunkServerHelper: Couldn't notify "+
                            "Controller of corruption. "+ioe.getMessage() );
      }
      return false;
    }

    // Chunk's filename is proper, the read was successful, and the file
    // wasn't corrupt -- we are now ready to serve the file
    // remove hashes from chunk and strip of metadata
    byte[] cleanBytes = FileDistributionService.getDataFromChunk(
        FileDistributionService.removeHashesFromChunk( fileBytes ) );
    ChunkServerServesFile response =
        new ChunkServerServesFile( filename, cleanBytes );
    // send raw data to requester
    try {
      connection.getSender().sendData( response.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "serveChunk: Couldn't serve '"+filename+"' to requester. "+
          ioe.getMessage() );
    }
    return true;
  }

  /**
   * Store the file sent for storage, and relay the message to the next
   * replication (or shard) server.
   *
   * @param event
   * @param connection
   */
  private void storeAndRelay(Event event, TCPConnection connection) {
    SendsFileForStorage message = ( SendsFileForStorage ) event;

    // Send acknowledgement that we received the file
    // MAY REMOVE LATER...
    try {
      sendGeneralMessage( Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_FOR_STORAGE,
          message.filename, connection );
    } catch ( IOException ioe ) {
      System.err.println(
          "storeAndRelay: Unable to send acknowledgement to sender "+"of '"+
          message.filename+"'. "+ioe.getMessage() );
    }

    byte[] fileBytes =
        FileDistributionService.readyFileForStorage( message.filename,
            message.data );
    if ( fileBytes == null ) {
      System.err.println(
          "storeAndRelay: There was a problem creating a byte string for"+
          " the file. Cannot save to disk." );
    } else {
      boolean saved =
          fileService.overwriteNewFile( message.filename, fileBytes );
      String not = saved ? "" : "NOT ";
      System.out.println(
          "storeAndRelay: '"+message.filename+"' was "+not+"stored." );
    }

    // Relay file to next ChunkServer in list
    message.nextServer += 1;
    if ( message.nextServer < message.servers.length ) {
      try {
        connectionCache.getConnection( this,
                           message.servers[message.nextServer], true )
                       .getSender()
                       .sendData( message.getBytes() );
      } catch ( IOException ioe ) {
        System.err.println(
            "storeAndRelay: Unable to relay store message to next "+
            "ChunkServer. "+ioe.getMessage() );
      }
    }

    // If boolean 'saved' is false (the file could not be stored), could
    // send message back to Controller that the store operation failed.
    // Then the Controller could find a suitable replacement home for the
    // file.

  }

  /**
   * Attempts to delete a file from this ChunkServer based on a request from the
   * Controller.
   *
   * @param event
   * @param connection
   */
  private void deleteRequestHelper(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();
    System.out.println( "deleteRequestHelper: Attempting to delete '"+filename+
                        "' from the ChunkServer." );
    try {
      fileService.deleteFile( filename );
    } catch ( IOException ioe ) {
      System.out.println(
          "deleteRequestHelper: Could not delete file. "+ioe.getMessage() );
    }
    try {
      sendGeneralMessage( Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE,
          filename, connection );
    } catch ( IOException ioe ) {
      System.err.println(
          "deleteRequestHelper: Unable to send acknowledgement of "+
          "deletion to Controller. "+ioe.getMessage() );
    }
  }

  /**
   * Interpret the response from the Controller to the registration request. If
   * successful, the ChunkServer's 'identifier' member will be set to the one
   * given by the Controller, the directory into which the ChunkServer will
   * store files will be created, and the FileDistributionService and
   * HeartbeatService will be started.
   *
   * @param event
   */
  private void registrationInterpreter(Event event) {
    GeneralMessage report = ( GeneralMessage ) event;
    int status = Integer.parseInt( report.getMessage() );
    if ( status == -1 ) {
      System.out.println(
          "registrationInterpreter: Controller denied the registration "+
          "request." );
    } else {
      if ( registrationSetup( status ) ) {
        System.out.println(
            "registrationInterpreter: Controller has approved the"+" "+
            "registration request. Our identifier is "+status+"." );
      } else {
        System.err.println(
            "registrationInterpreter: Though the Controller approved "+
            "our registration request, there was a problem "+
            "setting up the FileDistributionService and "+
            "HeartbeatService. Sending "+"deregistration back to Controller." );
        try {
          sendGeneralMessage( Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION,
              String.valueOf( status ), controllerConnection );
        } catch ( IOException ioe ) {
          System.err.println( "registrationInterpreter: Unable to "+
                              "send deregistration request to Controller. "+
                              ioe.getMessage() );
        }
      }
    }
  }

  /**
   * After receiving a successful registration response from the Controller,
   * start the FileDistributionService and HeartbeatService in a unique
   * directory in the file system's /tmp folder.
   *
   * @param identifier controller has assigned this ChunkServer
   * @return true if all actions completed successfully, false otherwise
   */
  private boolean registrationSetup(int identifier) {
    try {
      this.identifier = identifier;
      fileService = new FileDistributionService( identifier );
      HeartbeatService heartbeatService = new HeartbeatService( this );
      // create timer to schedule heartbeatService to run once every
      // Constants.HEARTRATE milliseconds, give it a random offset to
      // start
      heartbeatTimer = new Timer();
      long randomOffset = ThreadLocalRandom.current()
                                           .nextInt( 2,
                                               (Constants.HEARTRATE/2000)+1 );
      heartbeatTimer.scheduleAtFixedRate( heartbeatService, randomOffset*1000L,
          Constants.HEARTRATE );
    } catch ( Exception e ) {
      System.err.println(
          "registrationInterpreter: There was a problem setting up "+
          "the ChunkServer for operation after it had "+"been registered. "+
          e.getMessage() );
      if ( fileService != null ) {
        fileService = null;
      }
      if ( heartbeatTimer != null ) {
        heartbeatTimer.cancel();
        heartbeatTimer = null;
      }
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

    GeneralMessage generalMessage = new GeneralMessage( type, message );
    connection.getSender().sendData( generalMessage.getBytes() );
  }

  /**
   * Loops for user input at the ChunkServer.
   */
  private void interact() {
    System.out.println(
        "Enter a command or use 'help' to print a list of commands." );
    Scanner scanner = new Scanner( System.in );
    while ( true ) {
      String command = scanner.nextLine();
      String[] splitCommand = command.split( "\\s+" );
      switch ( splitCommand[0].toLowerCase() ) {
        default:
          System.err.println( "Unrecognized command. Use 'help' command." );
          break;
      }
    }
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
   * Returns the fileService running on this ChunkServer.
   *
   * @return fileService (FileDistributionService)
   */
  public FileDistributionService getFileService() {
    return fileService;
  }

  /**
   * Returns the TCPConnection associated with the Controller.
   *
   * @return TCPConnection with Controller
   */
  public TCPConnection getControllerConnection() {
    return controllerConnection;
  }
}