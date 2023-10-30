package cs555.overlay.node;

import cs555.overlay.files.*;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.*;
import cs555.overlay.wireformats.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.Timer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkServer implements Node {

  private final String host;
  private final int port;
  private final TCPConnectionCache connectionCache;

  // members that are set upon successful registration with Controller
  private final AtomicBoolean isRegistered;
  private TCPConnection controllerConnection;
  private int identifier;
  private FileSynchronizer synchronizer;
  private Timer heartbeatTimer;

  public ChunkServer(String host, int port) {
    this.host = host;
    this.port = port;
    this.connectionCache = new TCPConnectionCache();
    this.isRegistered = new AtomicBoolean( false );
  }

  /**
   * Entry point for the ChunkServer. Creates a ServerSocket with optional port
   * as a command line argument, connects to the Controller, sends the
   * Controller a registration request, and then loops for user commands.
   *
   * @param args port for ServerSocket (optional)
   */
  public static void main(String[] args) {
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

      case Protocol.REQUESTS_SHARD:
      case Protocol.REQUESTS_CHUNK:
        serveFile( event, connection );
        break;

      case Protocol.CONTROLLER_SENDS_HEARTBEAT:
        acknowledgeHeartbeat( connection );
        break;

      case Protocol.REPAIR_CHUNK:
        repairChunkHelper( event );
        break;

      case Protocol.REPAIR_SHARD:
        repairShardHelper( event );
        break;

      default:
        System.err.println( "Event couldn't be processed. "+event.getType() );
        break;
    }
  }

  /**
   * If this ChunkServer is this message's destination, tries to repair its
   * reconstruct its local shard from the fragments in the message. If it isn't
   * the destination, tries to attach its own shard to the message to relay.
   *
   * @param event message being processed
   */
  private void repairShardHelper(Event event) {
    RepairShard repairMessage = ( RepairShard ) event;
    ShardReader shardReader = new ShardReader( repairMessage.getFilename() );
    shardReader.readAndProcess( synchronizer );

    // If we are the target in the repair
    if ( repairMessage.getDestination().equals( host+":"+port ) ) {
      if ( shardReader.isCorrupt() ) { // And if the shard is corrupt
        boolean repaired = repairAndWriteShard( repairMessage, shardReader );
        String succeeded = repaired ? "" : "NOT";
        System.out.println(
            "repairShardHelper: '"+repairMessage.getFilename()+"' was "+
            succeeded+" repaired." );
      }
    } else { // try to add our uncorrupted shard
      contributeToShardRepair( repairMessage, shardReader );
      // Forward message or send directly to destination
      String nextServer;
      if ( repairMessage.fragmentsCollected() >= Constants.DATA_SHARDS ||
           !repairMessage.nextPosition() ) {
        nextServer = repairMessage.getDestination();
        repairMessage.setPositionToDestination(); // new, and necessary
      } else {
        nextServer = repairMessage.getAddress();
      }
      try {
        connectionCache.getConnection( this, nextServer, false )
                       .getSender()
                       .sendData( repairMessage.getBytes() );
      } catch ( IOException ioe ) {
        System.err.println(
            "repairShardHelper: Message couldn't be forwarded. "+
            ioe.getMessage() );
      }
    }
  }

  /**
   * Attaches local fragment to the repair message if the fragment isn't
   * corrupt.
   *
   * @param repairMessage to attach our fragment to
   * @param shardReader that was used to read the local fragment
   */
  private void contributeToShardRepair(RepairShard repairMessage,
      ShardReader shardReader) {
    if ( !shardReader.isCorrupt() ) { // if our own shard isn't corrupt
      int fragmentIndex =
          Integer.parseInt( shardReader.getFilename().split( "_shard" )[1] );
      // attach local fragment to correct fragment index (parsed from filename)
      repairMessage.attachFragment( fragmentIndex, shardReader.getData() );
    }
  }

  /**
   * Uses the shard fragments attached to the message to attempt to repair the
   * local corrupt fragment, and write it to disk.
   *
   * @param repairMessage received from another ChunkServer
   * @param shardReader that was used to read the local fragment
   * @return true if repaired fragment was written to disk, false otherwise
   */
  private boolean repairAndWriteShard(RepairShard repairMessage,
      ShardReader shardReader) {
    ShardWriter shardWriter = new ShardWriter( shardReader );
    shardWriter.setReconstructionShards( repairMessage.getFragments() );
    try {
      shardWriter.prepare();
      return shardWriter.write( synchronizer );
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println( "repairAndWriteShard: SHA1 unavailable. '"+
                          repairMessage.getFilename()+
                          "' could not be repaired."+nsae.getMessage() );
    }
    return false;
  }

  /**
   * If this ChunkServer is this message's destination, tries to repair its
   * local chunk with the replacement slices in the message. If it isn't the
   * destination, tries to add its local non-corrupt chunk slices to the message
   * to be relayed.
   *
   * @param event message being processed
   */
  private void repairChunkHelper(Event event) {
    RepairChunk repairMessage = ( RepairChunk ) event;
    ChunkReader chunkReader = new ChunkReader( repairMessage.getFilename() );
    chunkReader.readAndProcess( synchronizer );

    // If we are the target for the repair
    if ( repairMessage.getDestination().equals( host+":"+port ) ) {
      if ( chunkReader.isCorrupt() ) { // And if the chunk is corrupt
        boolean repaired = repairAndWriteChunk( repairMessage, chunkReader );
        String succeeded = repaired ? "" : "NOT";
        System.out.println(
            "repairChunkHelper: '"+repairMessage.getFilename()+"' was "+
            succeeded+" repaired." );
      }
    } else { // Try to attach uncorrupted slices and relay the message
      contributeToChunkRepair( repairMessage, chunkReader );
      String nextServer;
      if ( repairMessage.allSlicesRetrieved() ||
           !repairMessage.nextPosition() ) { // send to destination
        nextServer = repairMessage.getDestination();
      } else { // send to next server in chain
        nextServer = repairMessage.getAddress();
      }
      // Attempt to pass on the message
      try {
        connectionCache.getConnection( this, nextServer, false )
                       .getSender()
                       .sendData( repairMessage.getBytes() );
      } catch ( IOException ioe ) {
        System.err.println(
            "repairChunkHelper: Message couldn't be forwarded. "+
            ioe.getMessage() );
      }
    }

  }

  /**
   * Adds local non-corrupt slices of the chunk read by the ChunkReader into the
   * RepairMessage.
   *
   * @param repairMessage being added to
   * @param chunkReader that non-corrupt slices are being taken from
   */
  private void contributeToChunkRepair(RepairChunk repairMessage,
      ChunkReader chunkReader) {
    int[] localCorruptSlices = chunkReader.getCorruption(); // will be null
    // if no slices are corrupt
    byte[][] localSlices = chunkReader.getSlices();
    int[] slicesNeedingRepair = repairMessage.slicesStillNeedingRepair();
    for ( int index : slicesNeedingRepair ) {
      // 'contains' function always returns false if localCorruptSlices=null
      if ( !ArrayUtilities.contains( localCorruptSlices, index ) ) {
        repairMessage.attachSlice( index, localSlices[index] );
      }
    }
  }

  /**
   * Replaces any corrupt slices of a chunk that has been read from the disk
   * with non-corrupt replacement slices from the RepairMessage. Then attempts
   * to write the repaired chunk to disk.
   *
   * @param repairMessage received from another ChunkServer
   * @param chunkReader used to read the Chunk that needs repairing
   * @return true if successfully wrote repaired chunk to disk, false otherwise
   */
  private boolean repairAndWriteChunk(RepairChunk repairMessage,
      ChunkReader chunkReader) {
    ChunkWriter chunkWriter = new ChunkWriter( chunkReader );
    for ( int i : repairMessage.getRepairedIndices() ) {
      System.out.print( i+"," );
    }
    chunkWriter.setReplacementSlices( repairMessage.getRepairedIndices(),
        repairMessage.getReplacedSlices() );
    try {
      chunkWriter.prepare();
      return chunkWriter.write( synchronizer );
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println( "repairAndWriteChunk: SHA1 unavailable. '"+
                          repairMessage.getFilename()+
                          "' could not be repaired."+nsae.getMessage() );
    }
    return false;
  }

  /**
   * Responds to Controller's heartbeat message. In the Controller, these
   * messages it sends to ChunkServers are called 'pokes', and the responses it
   * receives are called 'pokeReplies'. A count of each is kept in the
   * ServerConnection of every registrant, and if the discrepancy between the
   * two counts becomes too great, the ChunkServer is automatically
   * deregistered.
   *
   * @param connection that sent the message (should be controllerConnection)
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
   * Attempts to read the requested file from disk, and serves it if it isn't
   * corrupt. If it is corrupt, contacts the Controller, and denies the
   * request.
   *
   * @param event message being processed
   * @param connection that sent the message
   */
  private void serveFile(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();

    // Read the file, READER MIGHT BE NULL!
    FileReaderFactory factory = FileReaderFactory.getInstance();
    FileReader reader = factory.createFileReader( filename );
    reader.readAndProcess( synchronizer );

    // Notify Controller of corruption and deny request
    if ( reader.isCorrupt() ) {
      ChunkServerReportsFileCorruption corruptionMessage =
          new ChunkServerReportsFileCorruption( identifier, filename,
              reader.getCorruption() );
      try {
        controllerConnection.getSender()
                            .sendData( corruptionMessage.getBytes() );
      } catch ( IOException ioe ) {
        System.err.println(
            "serveFile: Controller could not be notified of corruption. "+
            ioe.getMessage() );
      }
      try {
        sendGeneralMessage( Protocol.CHUNK_SERVER_DENIES_REQUEST, filename,
            connection );
      } catch ( IOException ioe ) {
        System.err.println(
            "serveFile: Connection could not be notified of of denial. "+
            ioe.getMessage() );
      }
      return;
    }

    // Serve the file
    ChunkServerServesFile serveMessage =
        new ChunkServerServesFile( filename, reader.getData() );
    try {
      connection.getSender().sendData( serveMessage.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "serveFile: Unable to serve file to connection. "+ioe.getMessage() );
    }
  }

  /**
   * Stores the file sent for storage, and relays the message to the next
   * ChunkServer.
   *
   * @param event message being processed
   * @param connection that sent the message
   */
  private void storeAndRelay(Event event, TCPConnection connection) {
    SendsFileForStorage message = ( SendsFileForStorage ) event;

    // Send acknowledgement that we received the file?
    // MAY REMOVE LATER...
    try {
      sendGeneralMessage( Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_FOR_STORAGE,
          message.getFilename(), connection );
    } catch ( IOException ioe ) {
      System.err.println(
          "storeAndRelay: Unable to send acknowledgement to sender of '"+
          message.getFilename()+"'. "+ioe.getMessage() );
    }

    // Try to write the file to disk
    FileWriterFactory factory = FileWriterFactory.getInstance();
    // WRITER WILL BE NULL IF FILENAME INCORRECT
    FileWriter writer =
        factory.createFileWriter( message.getFilename(), message.getContent() );
    boolean writtenSuccessfully = false;
    try {
      writer.prepare();
      writtenSuccessfully = writer.write( synchronizer );
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println(
          "storeAndRelay: SHA1 is not available. "+nsae.getMessage() );
    }

    String not = writtenSuccessfully ? "" : "NOT ";
    System.out.println(
        "storeAndRelay: '"+message.getFilename()+"' was "+not+"stored." );

    // Relay file to next ChunkServer in list
    if ( message.nextPosition() ) {
      try {
        connectionCache.getConnection( this, message.getServer(), true )
                       .getSender()
                       .sendData( message.getBytes() );
      } catch ( IOException ioe ) {
        System.err.println(
            "storeAndRelay: Unable to relay message to next ChunkServer. "+
            ioe.getMessage() );
      }
    }

    // If boolean 'writtenSuccessfully ' is false (the file could not be
    // stored), could send message back to Controller that the store
    // operation failed. Then the Controller could find a suitable
    // replacement home for the file.

  }

  /**
   * Attempts to delete a file from this ChunkServer apropos a request from the
   * Controller.
   *
   * @param event message being processed
   * @param connection that sent the message
   */
  private void deleteRequestHelper(Event event, TCPConnection connection) {
    String filename = (( GeneralMessage ) event).getMessage();
    System.out.println( "deleteRequestHelper: Attempting to delete '"+filename+
                        "' from the ChunkServer." );
    try {
      synchronizer.deleteFile( filename );
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
   * Interprets the Controller's response to the registration request. In a
   * successful registration, the ChunkServer's 'identifier' member will be set
   * to the one given by the Controller, the directory into which the
   * ChunkServer will store files will be created, and the HeartbeatService will
   * be started.
   *
   * @param event message being processed
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
            "registrationInterpreter: Though the Controller approved our "+
            "registration request, there was a problem setting up the "+
            "FileSynchronizer and HeartbeatService. Sending "+
            "deregistration back to Controller." );
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
   * creates the FileSynchronizer and starts the HeartbeatService in a unique
   * directory in the file system's /tmp folder.
   *
   * @param identifier controller has assigned this ChunkServer
   * @return true if all actions completed successfully, false otherwise
   */
  private boolean registrationSetup(int identifier) {
    try {
      this.identifier = identifier;
      synchronizer = new FileSynchronizer( identifier );
      HeartbeatService heartbeatService = new HeartbeatService( this );
      // create timer to schedule heartbeatService to run once every
      // Constants.HEARTRATE milliseconds, give it a random offset to start
      heartbeatTimer = new Timer();
      long randomOffset = ThreadLocalRandom.current()
                                           .nextInt( 2,
                                               (Constants.HEARTRATE/2000)+1 );
      heartbeatTimer.scheduleAtFixedRate( heartbeatService, randomOffset*1000L,
          Constants.HEARTRATE );
      isRegistered.set( true ); // set the registered status
    } catch ( Exception e ) {
      System.err.println(
          "registrationInterpreter: There was a problem setting up "+
          "the ChunkServer for operation after it had been registered. "+
          e.getMessage() );
      if ( synchronizer != null ) {
        synchronizer = null;
      }
      if ( heartbeatTimer != null ) {
        heartbeatTimer.cancel();
        heartbeatTimer = null;
      }
      isRegistered.set( false );
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
    interactLoop:
    while ( true ) {
      String command = scanner.nextLine();
      String[] splitCommand = command.split( "\\s+" );
      switch ( splitCommand[0].toLowerCase() ) {

        case "info":
          info();
          break;

        case "list":
          listFiles();
          break;

        case "exit":
          deregister();
          break interactLoop;

        case "help":
          showHelp();
          break;

        default:
          System.err.println( "Unrecognized command. Use 'help' command." );
          break;
      }
    }
    // Should try to gracefully shut down here
    // Close all TCPConnections
    // Cancel the heartbeat timer
    heartbeatTimer.cancel();
    System.exit( 0 );
  }

  /**
   * Print server address of this ChunkServer.
   */
  private void info() {
    System.out.printf( "%3s%s%n", "", host+":"+port );
  }

  /**
   * Send deregistration request to the Controller.
   */
  private void deregister() {
    try {
      sendGeneralMessage( Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION,
          String.valueOf( identifier ), controllerConnection );
    } catch ( IOException ioe ) {
      System.err.println( "deregister: Couldn't send deregistration request "+
                          "to the Controller. "+ioe.getMessage() );
    }
  }

  /**
   * Prints a list of files stored at this ChunkServer with valid chunk/shard
   * filenames.
   */
  private void listFiles() {
    String[] fileList;
    try {
      fileList = synchronizer.listFiles();
    } catch ( IOException ioe ) {
      System.err.println( "listFiles: There was a problem generating a list "+
                          "of stored files. "+ioe.getMessage() );
      return;
    }
    if ( fileList != null ) {
      for ( String filename : fileList ) {
        System.out.printf( "%3s%s%n", "", filename );
      }
    }
  }

  /**
   * Prints a list of valid commands.
   */
  private void showHelp() {
    System.out.printf( "%3s%-5s : %s%n", "", "info",
        "print host:port server address of this ChunkServer" );
    System.out.printf( "%3s%-5s : %s%n", "", "list",
        "print a list of files stored at this ChunkServer" );
    System.out.printf( "%3s%-5s : %s%n", "", "exit",
        "attempt to deregister and shutdown the ChunkServer" );
    System.out.printf( "%3s%-5s : %s%n", "", "help",
        "print a list of valid commands" );
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
   * Returns the synchronizer running on this ChunkServer.
   *
   * @return synchronizer (FileSynchronizer)
   */
  public FileSynchronizer getFileSynchronizer() {
    return synchronizer;
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