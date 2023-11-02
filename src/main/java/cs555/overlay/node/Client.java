package cs555.overlay.node;

import cs555.overlay.transport.TCPConnection;
import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.ClientReader;
import cs555.overlay.util.ClientWriter;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.wireformats.*;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client node in the DFS. It is responsible for parsing commands from the
 * user to store or retrieve files (mostly). It can parse various other
 * commands too.
 *
 * @author hayne
 */
public class Client implements Node {

  private final int storageType;
  private TCPConnection controllerConnection;
  private final ConcurrentMap<String, ClientWriter> writers;
  private final ConcurrentMap<String, ClientReader> readers;
  private Path workingDirectory;

  /**
   * Default constructor.
   *
   * @param storageType 0 for replication, 1 for erasure coding
   */
  public Client(int storageType) {
    this.storageType = storageType;
    this.writers = new ConcurrentHashMap<>();
    this.readers = new ConcurrentHashMap<>();
    this.workingDirectory = Paths.get( System.getProperty( "user.dir" ) );
  }

  /**
   * Entry point for the Client. Creates a Client using the storageType
   * specified in the 'application.properties' file, and connects to the
   * Controller. The Client is a node, but it doesn't have an active
   * ServerSocket, so it doesn't have its own values for 'host' and 'port'.
   *
   * @param args ignored
   */
  public static void main(String[] args) {

    // Get storageType from 'application.properties' file
    int storageType;
    if ( ApplicationProperties.storageType.equalsIgnoreCase( "erasure" ) ) {
      storageType = 1;
    } else if ( ApplicationProperties.storageType.equalsIgnoreCase(
        "replication" ) ) {
      storageType = 0;
    } else {
      System.out.println(
          "storageType set in 'application.properties' file is neither "+
          "'replication' nor 'erasure', defaulting to 'replication'." );
      storageType = 0;
    }

    // Establish connection with Controller
    try ( Socket controllerSocket = new Socket(
        ApplicationProperties.controllerHost,
        ApplicationProperties.controllerPort ) ) {

      Client client = new Client( storageType );

      // Set Client's connection to Controller, start the receiver
      client.controllerConnection =
          new TCPConnection( client, controllerSocket );
      client.controllerConnection.start();

      // Loop for user interaction
      client.interact();
    } catch ( IOException ioe ) {
      System.err.println(
          "Connection to the Controller couldn't be established. "+
          ioe.getMessage() );
      System.exit( 1 );
    }
  }

  @Override
  public String getHost() {
    return "N/A";
  }

  @Override
  public int getPort() {
    return -1;
  }

  @Override
  public void onEvent(Event event, TCPConnection connection) {
    switch ( event.getType() ) {

      case Protocol.CHUNK_SERVER_SERVES_FILE:
        directFileToReader( event );
        break;

      case Protocol.CHUNK_SERVER_DENIES_REQUEST:
        String filename = (( GeneralMessage ) event).getMessage();
        System.err.println( "Request denied for "+filename+"." );
        break;

      case Protocol.CONTROLLER_SENDS_FILE_LIST:
        printFileList( event );
        break;

      case Protocol.CONTROLLER_RESERVES_SERVERS:
        notifyOfServers( event );
        break;

      case Protocol.CONTROLLER_DENIES_STORAGE_REQUEST:
        System.out.println( "The Controller has denied a storage request." );
        stopWriterAndRequestDelete( (( GeneralMessage ) event).getMessage() );
        break;

      case Protocol.CONTROLLER_SENDS_STORAGE_LIST:
        notifyOfStorageInfo( event );
        break;

      default:
        System.err.println( "Event couldn't be processed. "+event.getType() );
        break;
    }
  }

  private void directFileToReader(Event event) {
    ChunkServerServesFile serveMessage = ( ChunkServerServesFile ) event;
    String baseFilename =
        FilenameUtilities.getBaseFilename( serveMessage.getFilename() );
    ClientReader reader = readers.get( baseFilename );
    if ( reader != null ) {
      reader.addFile( serveMessage.getFilename(), serveMessage.getContent() );
    }
  }

  // Stopping the writer should be decoupled from request delete.

  /**
   * Stops the ClientWriter writing the file with name 'filename', and sends a
   * delete request to the Controller for that particular file.
   *
   * @param filename filename to stop the writer for and request delete of
   */
  private synchronized void stopWriterAndRequestDelete(String filename) {
    ClientWriter writer = writers.get( filename );
    if ( writer != null ) {
      writer.setServersAndNotify( null );
    }
    GeneralMessage requestDelete =
        new GeneralMessage( Protocol.CLIENT_REQUESTS_FILE_DELETE, filename );
    try {
      controllerConnection.getSender().sendData( requestDelete.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println( "stopWriterAndRequestDelete: Couldn't send "+
                          "Controller a request to delete '"+filename+"'."+" "+
                          ioe.getMessage() );
    }
  }

  /**
   * Will be called when a message containing addresses of ChunkServers has been
   * received by the Client. It sets the 'servers' member inside the relevant
   * ClientWriter, and unlocks it so it may send the chunk to those servers.
   *
   * @param event message being handled
   */
  private void notifyOfServers(Event event) {
    ControllerReservesServers message = ( ControllerReservesServers ) event;
    ClientWriter writer = writers.get( message.getFilename() );
    if ( writer != null ) {
      writer.setServersAndNotify( message.getServers() );
    }
  }

  /**
   * Will be called when a message containing the storage info for a particular
   * file is received by the Client. This function sets the 'servers' member for
   * the ClientReader of a particular file, thus unlocking it start reading the
   * file from the DFS.
   *
   * @param event message being handled
   */
  private void notifyOfStorageInfo(Event event) {
    ControllerSendsStorageList message = ( ControllerSendsStorageList ) event;
    ClientReader reader = readers.get( message.getFilename() );
    if ( reader != null ) {
      reader.setServersAndNotify( message.getServers() );
    }
  }

  /**
   * Print the list of files sent by the Controller.
   *
   * @param event message being processed
   */
  public void printFileList(Event event) {
    ControllerSendsFileList message = ( ControllerSendsFileList ) event;
    if ( message.getList() == null ) {
      System.out.printf( "%3s%s%n", "", "Controller: No files stored." );
    } else {
      for ( String filename : message.getList() ) {
        System.out.printf( "%3s%s%n", "", filename );
      }
    }
  }

  /**
   * Loops for user input at the Client.
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

        case "put":
          put( splitCommand );
          break;

        case "get":
          get( splitCommand );
          break;

        case "delete":
          requestFileDelete( splitCommand );
          break;

        case "stop":
          stopHelper( splitCommand );
          break;

        case "readers":
          showReaders();
          break;

        case "writers":
          showWriters();
          break;

        case "files":
          requestFileList();
          break;

        case "wd":
          printWorkingDirectory( splitCommand );
          break;

        case "exit":
          break interactLoop;

        case "help":
          showHelp();
          break;

        default:
          System.err.println( "Unrecognized command. Use 'help' command." );
          break;
      }
    }
    controllerConnection.close(); // Close the controllerConnection
    System.exit( 0 );
  }

  /**
   * Attempts to unlock the writer (assuming it is frozen) after it has set its
   * 'servers' member to null (which instructs the writer to return
   * immediately).
   *
   * @param command user input split by whitespace
   */
  private void stopHelper(String[] command) {
    if ( command.length > 1 ) {
      stopWriterAndRequestDelete( command[1] );
    } else {
      System.err.println( "stop: No filename given. Use 'help' for usage." );
    }
  }

  /**
   * Send a request to the Controller to delete a file.
   *
   * @param command user input split by whitespace
   */
  private void requestFileDelete(String[] command) {
    if ( command.length > 1 ) {
      for ( int i = 1; i < command.length; ++i ) {
        GeneralMessage deleteMessage =
            new GeneralMessage( Protocol.CLIENT_REQUESTS_FILE_DELETE,
                command[i] );
        try {
          controllerConnection.getSender().sendData( deleteMessage.getBytes() );
        } catch ( IOException ioe ) {
          System.err.println(
              "requestFileDelete: Couldn't send Controller a delete request. "+
              ioe.getMessage() );
        }
      }
    } else {
      System.err.println( "delete: No file given. Use 'help' for usage." );
    }
  }

  /**
   * Print a list of active writers.
   */
  private void showWriters() {
    writers.forEach(
        (k, v) -> System.out.printf( "%3s%3d%-2s%s%n", "", v.getProgress(), "%",
            k ) );
  }

  /**
   * Print a list of active readers.
   */
  private void showReaders() {
    readers.forEach(
        (k, v) -> System.out.printf( "%3s%3d%-2s%s%n", "", v.getProgress(), "%",
            k ) );
  }

  /**
   * Sends a message request to the Controller for a list of files stored on the
   * DFS.
   */
  private void requestFileList() {
    GeneralMessage requestMessage =
        new GeneralMessage( Protocol.CLIENT_REQUESTS_FILE_LIST );
    try {
      controllerConnection.getSender().sendData( requestMessage.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "requestFileList: Could not send a file list request to the "+
          "Controller. "+ioe.getMessage() );
    }
  }

  /**
   * Either prints the current value of 'workingDirectory', or tries to set it
   * based on input from the user.
   *
   * @param command String[] of command from user
   */
  private void printWorkingDirectory(String[] command) {
    if ( command.length > 1 ) {
      setWorkingDirectory( command[1] );
    }
    System.out.printf( "%3s%s%n", "", workingDirectory.toString() );
  }

  /**
   * Takes a string representing a possible path and converts it to a path
   * object, replacing a leading tilde with the home directory, if possible.
   *
   * @param pathString String of possible path
   * @return Path object based on the pathString
   */
  private Path parsePath(String pathString) {
    Path parsedPath;
    if ( pathString.startsWith( "~" ) ) {
      pathString = pathString.replaceFirst( "~", "" );
      pathString = removeLeadingFileSeparators( pathString );
      parsedPath = Paths.get( System.getProperty( "user.home" ) );
    } else {
      parsedPath = workingDirectory;
    }
    if ( !pathString.isEmpty() ) {
      parsedPath = parsedPath.resolve( pathString );
    }
    return parsedPath.normalize(); // clean up path
  }

  /**
   * Sets the working directory based on input from the user.
   *
   * @param workdir new desired working directory
   */
  private void setWorkingDirectory(String workdir) {
    workingDirectory = parsePath( workdir );
  }

  /**
   * Removes leading file separators from a String. Helps to make sure calls
   * like 'pwd ~/' and 'pwd ~' and 'pwd ~///' all evaluate to the home
   * directory.
   *
   * @param directory String to be modified
   * @return string leading file separators removed
   */
  private String removeLeadingFileSeparators(String directory) {
    while ( !directory.isEmpty() && directory.startsWith( File.separator ) ) {
      directory = directory.substring( 1 );
    }
    return directory;
  }

  /**
   * Attempts to store a file on the DFS.
   *
   * @param command String[] of command given by user, split by space
   */
  private void put(String[] command) {
    if ( command.length < 2 ) {
      System.err.println( "put: No file given. Use 'help' for usage. " );
      return;
    }
    Path pathToFile = parsePath( command[1] );
    if ( writers.get( pathToFile.toString() ) != null ) {
      System.err.println(
          "That file currently has an active writer. If the writer is frozen, "+
          "interrupt it first, and try again." );
      return;
    } else if ( readers.get( command[1] ) != null ) {
      System.err.println(
          "That file currently has an active reader. Writing it before "+
          "the reader has finished will interleave metadata at the "+
          "Controller." );
      return;
    }
    ClientWriter writer = new ClientWriter( this, pathToFile );
    writers.put( pathToFile.getFileName().toString(), writer );
    (new Thread( writer )).start();
    // Make sure the file is a file
    // Get exclusive lock on entire file
    // Read all of file's content into memory, or do it one chunk at a time?
    // Try to allocate servers by messaging the Controller, use storageType
    // Need to wait for reply somehow...
    // controllerConnection will enter onEvent, somehow the message has to
    // get to the waiting thread...
  }

  /**
   * Attempts to retrieve a file from the DFS.
   *
   * @param command String[] of command given by user, split by space
   */
  private void get(String[] command) {
    if ( command.length < 2 ) {
      System.err.println( "get: No filename given. Use 'help' for usage. " );
      return;
    } else if ( readers.containsKey( command[1] ) ) {
      System.err.println(
          "That file currently has an active reader. If the reader is frozen, "+
          "interrupt it first, and try again." );
      return;
    } else if ( writers.containsKey( command[1] ) ) {
      System.err.println(
          "That file currently has an active writer. The file shouldn't be "+
          "read until it has been written." );
      return;
    }
    ClientReader reader = new ClientReader( this, command[1] );
    readers.put( command[1], reader );
    (new Thread( reader )).start();
  }

  /**
   * Prints a list of valid commands.
   */
  private void showHelp() {
    System.out.printf( "%3s%-19s : %s%n", "", "put path/local_file",
        "store a local file on the DFS" );
    System.out.printf( "%3s%-19s : %s%n", "", "get filename",
        "retrieve a file from the DFS" );
    System.out.printf( "%3s%-19s : %s%n", "", "delete file1 file2",
        "request that file(s) be deleted from the DFS" );
    System.out.printf( "%3s%-19s : %s%n", "", "stop filename",
        "stops writer for 'filename' and sends delete request" );
    System.out.printf( "%3s%-19s : %s%n", "", "writers",
        "display list files in the process of being stored" );
    System.out.printf( "%3s%-19s : %s%n", "", "readers",
        "display list files in the process of being retrieved" );
    System.out.printf( "%3s%-19s : %s%n", "", "files",
        "print a list of files stored on the DFS" );
    System.out.printf( "%3s%-19s : %s%n", "", "wd [new_workdir]",
        "print the current working directory or change it" );
    System.out.printf( "%3s%-19s : %s%n", "", "exit",
        "disconnect from the Controller and shutdown the Client" );
    System.out.printf( "%3s%-19s : %s%n", "", "help",
        "print a list of valid commands" );
  }

  /**
   * Remove a writer from the ClientWriter hashmap. Writers call this at the end
   * of their run.
   *
   * @param pathToFile filename key of writer
   */
  public void removeWriter(String pathToFile) {
    writers.remove( pathToFile );
  }

  /**
   * Remove a reader from the ClientReader hashmap. Readers call this at the end
   * of their run.
   *
   * @param filename filename key of writer
   */
  public void removeReader(String filename) {
    readers.remove( filename );
  }

  /**
   * Getter for the controllerConnection.
   *
   * @return controllerConnection
   */
  public TCPConnection getControllerConnection() {
    return controllerConnection;
  }

  /**
   * Getter for storageType
   *
   * @return 0 for replication, 1 for erasure coding
   */
  public int getStorageType() {
    return storageType;
  }
}