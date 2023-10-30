package cs555.overlay.node;

import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.ClientWriter;
import cs555.overlay.wireformats.*;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Client implements Node {

  private final int storageType;
  private TCPConnection controllerConnection;
  private final TCPConnectionCache connectionCache;
  private final ConcurrentMap<String, ClientWriter> writers;
  //private Map<String, ClientReader> readers;
  private Path workingDirectory;

  /**
   * Default constructor.
   *
   * @param storageType 0 for replication, 1 for erasure coding
   */
  public Client(int storageType) {
    this.storageType = storageType;
    this.connectionCache = new TCPConnectionCache();
    this.writers = new ConcurrentHashMap<>();
    //this.readers = new HashMap<>();
    this.workingDirectory = Paths.get( System.getProperty( "user.dir" ) );
  }
  //
  //  private static byte[] getShardFromServer(String filename, String address,
  //      Map<String, TCPSender> tcpConnections) {
  //    TCPSender connection = getTCPSender( tcpConnections, address );
  //    if ( connection == null ) {
  //      System.out.println( "Couldn't establish a connection." );
  //      return null;
  //    }
  //    try {
  //      RequestsShard request = new RequestsShard( filename );
  //      connection.sendData( request.getBytes() );
  //      byte[] reply = connection.receiveData();
  //      if ( reply == null ) {
  //        //System.out.println("Null Reply");
  //        return null;
  //      } else if ( reply[0] == Protocol.CHUNK_SERVER_DENIES_REQUEST ) {
  //        //System.out.println("CHUNK_SERVER_DENIES_REQUEST");
  //        return null;
  //      }
  //      ChunkServerServesFile serve = new ChunkServerServesFile( reply );
  //      //System.out.println(serve.filedata.length);
  //      return serve.filedata;
  //    } catch ( Exception e ) {
  //      return null;
  //    }
  //  }
  //
  //  private static byte[] getChunkFromServer(String filename, String address,
  //      Map<String, TCPSender> tcpConnections) {
  //    TCPSender connection = getTCPSender( tcpConnections, address );
  //    if ( connection == null ) {
  //      System.out.println( "Couldn't esablish a connection." );
  //      return null;
  //    }
  //    try {
  //      RequestsChunk request = new RequestsChunk( filename );
  //      connection.sendData( request.getBytes() );
  //      byte[] reply = connection.receiveData();
  //      if ( reply == null || reply[0] == Protocol
  //      .CHUNK_SERVER_DENIES_REQUEST ) {
  //        return null;
  //      }
  //      ChunkServerServesFile serve = new ChunkServerServesFile( reply );
  //      return serve.filedata;
  //    } catch ( Exception e ) {
  //      return null;
  //    }
  //  }
  //
  //  private static byte[] getChunkFromReplicationServers(String filename,
  //      String[] servers, Map<String, TCPSender> tcpConnections) {
  //    if ( servers == null ) {
  //      return null;
  //    }
  //    for ( String server : servers ) {
  //      //System.out.println(server);
  //      byte[] data = getChunkFromServer( filename, server, tcpConnections );
  //      if ( data != null ) {
  //        return data;
  //      }
  //    }
  //    return null;
  //  }
  //
  //  // filename will be filename_chunk#, so must append "_shard#"
  //  private static byte[][] getShardsFromServers(String filename,
  //      String[] servers, Map<String, TCPSender> tcpConnections) {
  //    //for (String server : servers) System.out.println(server);
  //    if ( servers == null ) {
  //      return null;
  //    }
  //    byte[][] shards = new byte[Constants.TOTAL_SHARDS][];
  //    int index = -1;
  //    for ( String server : servers ) {
  //      index++;
  //      if ( server == null ) {
  //        continue;
  //      }
  //      String[] parts = server.split( ":" );
  //      String shardName = filename+"_shard"+index;
  //      byte[] fileData = getShardFromServer( shardName, server,
  //      tcpConnections );
  //      if ( fileData == null ) {
  //        continue;
  //      }
  //      shards[index] = fileData;
  //    }
  //    return FileSynchronizer.decodeMissingShards( shards );
  //  }
  //
  //  public static TCPSender getTCPSender(Map<String, TCPSender>
  //  tcpConnections,
  //      String address) {
  //    if ( tcpConnections.containsKey( address ) ) {
  //      return tcpConnections.get( address );
  //    }
  //    try {
  //      String hostname = address.split( ":" )[0];
  //      int port = Integer.valueOf( address.split( ":" )[1] );
  //      Socket socket = new Socket( hostname, port );
  //      socket.setSoTimeout( 3000 );
  //      TCPSender newConnection = new TCPSender( socket );
  //      tcpConnections.put( address, newConnection );
  //      return newConnection;
  //    } catch ( ConnectException|UnknownHostException ce ) {
  //      System.out.println( "Failed to connect: "+ce );
  //      return null;
  //    } catch ( IOException ioe ) {
  //      System.out.println( "The connection was terminated: "+ioe );
  //      ioe.printStackTrace();
  //      return null;
  //    }
  //  }
  //
  //  private static void store(int schema, String filename,
  //      Map<String, TCPSender> tcpConnections) {
  //    Path path = Paths.get( filename );
  //    Path name = path.getFileName();
  //    String basename = name.toString();
  //    String address = ApplicationProperties.controllerHost+":"+
  //                     ApplicationProperties.controllerPort;
  //    TCPSender connection = getTCPSender( tcpConnections, address );
  //    if ( connection == null ) {
  //      System.out.println(
  //          "Couldn't establish a connection with the Controller." );
  //      return;
  //    }
  //    try {
  //      double totalChunks =
  //          Math.ceil( FileSynchronizer.getFileSize( filename )
  //          /65536 );
  //      double tenth = totalChunks/10, prints = 0; // For updating an upload
  //      // progress bar
  //      //System.out.println( "totalChunks: " + totalChunks + ", tenth: " +
  //      // tenth );
  //      int index = 0;
  //      boolean finished = false;
  //      while ( !finished ) {
  //        byte[] newchunk =
  //            FileSynchronizer.getNextChunkFromFile( filename, index );
  //        if ( newchunk == null ) {
  //          finished = true;
  //          break;
  //        }
  //        // Printing an upload progress bar
  //        if ( prints == 0 ) {
  //          System.out.print( "Uploading: 0%.." );
  //          ++prints;
  //        } else {
  //          if ( index >= prints*tenth && prints != 10 ) {
  //            System.out.print( (( int ) prints*10)+"%.." );
  //            ++prints;
  //          }
  //        }
  //        if ( !finished ) {
  //          boolean sentToServers = false;
  //          if ( schema == 0 ) { // We are replicating
  //            ClientRequestsStoreChunk request =
  //                new ClientRequestsStoreChunk( basename, index );
  //            connection.sendData( request.getBytes() );
  //            byte[] data = connection.receiveData();
  //            if ( data == null ) {
  //              System.out.println(
  //                  "\nNo message received from Controller for chunk "+index+
  //                  "." );
  //              finished = false;
  //              break;
  //            } else if ( data[0] ==
  //                        Protocol.CONTROLLER_DENIES_STORAGE_REQUEST ) {
  //              System.out.println(
  //                  "\nThe Controller denied the storage request of chunk
  //                  "+index+
  //                  "." );
  //              finished = false;
  //              break;
  //            }
  //            ControllerSendsClientValidChunkServers response =
  //                new ControllerSendsClientValidChunkServers( data );
  //            // Now need to send the newchunk to the first available Chunk
  //            // Server in the list.
  //            for ( int i = 0; i < response.servers.length; i++ ) {
  //              TCPSender serverConnection =
  //                  getTCPSender( tcpConnections, response.servers[i] );
  //              if ( serverConnection == null ) {
  //                continue;
  //              }
  //              try {
  //                String chunkFilename =
  //                    basename+"_chunk"+index;
  //                String[] forwardServers = new String[response.servers
  //                .length-1];
  //                int addIndex = 0;
  //                for ( int j = 0; j < response.servers.length; j++ ) {
  //                  if ( j != i ) {
  //                    forwardServers[addIndex] = response.servers[j];
  //                    addIndex++;
  //                  }
  //                }
  //                SendsFileForStorage storeChunk =
  //                    new SendsFileForStorage( chunkFilename, newchunk,
  //                        forwardServers );
  //                serverConnection.sendData( storeChunk.getBytes() );
  //                byte[] storeResponse = serverConnection.receiveData();
  //                if ( storeResponse == null ) {
  //                  continue;
  //                }
  //                ChunkServerAcknowledgesFileForStorage acknowledge =
  //                    new ChunkServerAcknowledgesFileForStorage(
  //                    storeResponse );
  //                sentToServers = true;
  //                break;
  //              } catch ( Exception e ) {
  //              }
  //            }
  //          } else { // We are sharding
  //            ClientRequestsStoreShards request =
  //                new ClientRequestsStoreShards( basename, index );
  //            connection.sendData( request.getBytes() );
  //            byte[] data = connection.receiveData();
  //            if ( data == null ) {
  //              System.out.println(
  //                  "\nNo message received from Controller for chunk "+index+
  //                  "." );
  //              finished = false;
  //              break;
  //            } else if ( data[0] ==
  //                        Protocol.CONTROLLER_DENIES_STORAGE_REQUEST ) {
  //              System.out.println(
  //                  "\nThe Controller denied the storage request of chunk
  //                  "+index+
  //                  "." );
  //              finished = false;
  //              break;
  //            }
  //            ControllerSendsClientValidShardServers response =
  //                new ControllerSendsClientValidShardServers( data );
  //            // Need to create shards
  //            byte[] chunkForStorage;
  //            try {
  //              chunkForStorage =
  //                  FileSynchronizer.readyChunkForStorage( index, 0,
  //                      newchunk );
  //            } catch ( Exception e ) {
  //              System.out.println( "\nstore: SHA1 is not available." );
  //              break;
  //            }
  //            byte[][] shards =
  //                FileSynchronizer.makeShardsFromChunk(
  //                chunkForStorage );
  //            for ( int i = 0; i < response.servers.length; i++ ) {
  //              TCPSender serverConnection =
  //                  getTCPSender( tcpConnections, response.servers[i] );
  //              if ( serverConnection == null ) {
  //                System.out.println( "\nCouldn't establish a connection
  //                with "+
  //                                    response.servers[i]+". Stopping." );
  //                break;
  //              }
  //              try {
  //                String shardFilename =
  //                    basename+"_chunk"+index+"_shard"+i;
  //                //System.out.println(shardFilename);
  //                SendsFileForStorage storeShard =
  //                    new SendsFileForStorage( shardFilename, shards[i],
  //                    null );
  //                serverConnection.sendData( storeShard.getBytes() );
  //                byte[] storeResponse = serverConnection.receiveData();
  //                if ( storeResponse == null ) { // Try the next server
  //                  System.out.println(
  //                      "\nShard server didn't acknowledge storage request
  //                      for '"+
  //                      shardFilename+"', stopping the storage operation." );
  //                  sentToServers = false;
  //                  break;
  //                }
  //              } catch ( Exception e ) {
  //                sentToServers = false;
  //                break;
  //              }
  //              if ( i == response.servers.length-1 ) {
  //                sentToServers = true;
  //              }
  //            }
  //          }
  //          if ( !sentToServers ) {
  //            break;
  //          }
  //        }
  //        index++;
  //      }
  //      if ( !finished ) {
  //        // Request to delete the file from the controller
  //        ClientRequestsFileDelete delete =
  //            new ClientRequestsFileDelete( basename );
  //        connection.sendData( delete.getBytes() );
  //        byte[] deleteResponse = connection.receiveData();
  //        if ( deleteResponse == null ) {
  //          System.out.println(
  //              "\nThe storage operation was unsuccessful. Controller
  //              didn't "+
  //              "respond to a request to delete the incomplete file." );
  //        } else if ( deleteResponse[0] ==
  //                    Protocol.CONTROLLER_APPROVES_FILE_DELETE ) {
  //          System.out.println(
  //              "\nThe storage operation was unsuccessful. Controller
  //              approved "+
  //              "the deletion of the incomplete file." );
  //        }
  //        return;
  //      }
  //      System.out.print( "100%\n" );
  //      System.out.println( "The storage operation was successful." );
  //      connection = null;
  //    } catch ( SocketTimeoutException ste ) {
  //      System.out.println( "\nSocket timed out: "+ste );
  //    } catch ( SocketException se ) {
  //      System.out.println( "\nSocket exception: "+se );
  //    } catch ( IOException ioe ) {
  //      System.out.println( "\nIOException: "+ioe );
  //    }
  //  }
  //

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

  //
  //  private String[] listFiles() {
  //    String address = ApplicationProperties.controllerHost+":"+
  //                     ApplicationProperties.controllerPort;
  //    TCPSender connection = getTCPSender( connections, address );
  //    if ( connection == null ) {
  //      System.out.println(
  //          "Couldn't esablish a connection with the Controller." );
  //      return new String[]{ "" };
  //    }
  //    try {
  //      ClientRequestsFileList listRequest = new ClientRequestsFileList();
  //      connection.sendData( listRequest.getBytes() );
  //      byte[] reply = connection.receiveData();
  //      if ( reply == null ) {
  //        System.out.println(
  //            "The Controller didn't respond to a request for files." );
  //        return new String[]{ "" };
  //      }
  //      ControllerSendsFileList list = new ControllerSendsFileList( reply );
  //      if ( list.list == null ) {
  //        return new String[]{ "" };
  //      }
  //      return list.list;
  //    } catch ( IOException ioe ) {
  //      System.out.println( "There was a problem receiving the file list." );
  //      return new String[]{ "" };
  //    }
  //  }
  //
  //  private void delete(String filename) {
  //    Path path = Paths.get( filename );
  //    Path name = path.getFileName();
  //    String basename = name.toString();
  //    String address = ApplicationProperties.controllerHost+":"+
  //                     ApplicationProperties.controllerPort;
  //    TCPSender connection = getTCPSender( connections, address );
  //    if ( connection == null ) {
  //      System.out.println(
  //          "Couldn't esablish a connection with the Controller." );
  //      return;
  //    }
  //    try {
  //      ClientRequestsFileDelete delete =
  //          new ClientRequestsFileDelete( basename );
  //      connection.sendData( delete.getBytes() );
  //      byte[] reply = connection.receiveData();
  //      if ( reply == null ) {
  //        System.out.println(
  //            "The Controller didn't respond to the delete request for file
  //            '"+
  //            basename+"'" );
  //      } else if ( reply[0] == Protocol.CONTROLLER_APPROVES_FILE_DELETE ) {
  //        System.out.println(
  //            "The Controller has acknowledged the request to delete file '"+
  //            basename+"'" );
  //      }
  //      connection = null;
  //    } catch ( SocketTimeoutException ste ) {
  //      System.out.println( "Socket timed out: "+ste );
  //    } catch ( SocketException se ) {
  //      System.out.println( "Socket exception: "+se );
  //    } catch ( IOException ioe ) {
  //      System.out.println( "IOException: "+ioe );
  //    }
  //  }
  //
  //  private void retrieve(String filename, String location) {
  //    Path path = Paths.get( filename );
  //    Path name = path.getFileName();
  //    String basename = name.toString();
  //    if ( !location.endsWith( "/" ) ) {
  //      location += "/";
  //    }
  //    File test = new File( location+basename );
  //    if ( test.exists() ) {
  //      System.out.println( "'"+location+basename+
  //                          "' already exists. This operation will append
  //                          it." );
  //    }
  //    String address = ApplicationProperties.controllerHost+":"+
  //                     ApplicationProperties.controllerPort;
  //    TCPSender connection = getTCPSender( connections, address );
  //    if ( connection == null ) {
  //      System.err.println(
  //          "Couldn't esablish a connection with the Controller." );
  //      return;
  //    }
  //    byte[] reply = null;
  //    try {
  //      ClientRequestsFileSize size = new ClientRequestsFileSize( basename );
  //      connection.sendData( size.getBytes() );
  //      reply = connection.receiveData();
  //      if ( reply == null ) {
  //        System.out.println(
  //            "The Controller didn't respond to the size request for file '"+
  //            basename+"'" );
  //        return;
  //      }
  //      ControllerReportsFileSize reportedSize =
  //          new ControllerReportsFileSize( reply ); // read for total chunks
  //      boolean finished = false;
  //      int lastChunk = 0;
  //      //System.out.println("total chunks: " + reportedSize.totalchunks);
  //      int tenth = reportedSize.totalChunks/10, prints = 0; // For updating a
  //      // download progress bar
  //      // Loop here for every chunk that needs retrieving
  //      for ( int i = 0; i < reportedSize.totalChunks; i++ ) {
  //        // For printing a progress bar for the download
  //        if ( prints == 0 ) {
  //          System.out.print( "Downloading: 0%.." );
  //          ++prints;
  //        } else {
  //          if ( i >= prints*tenth && prints != 10 ) {
  //            System.out.print( (prints*10)+"%.." );
  //            ++prints;
  //          }
  //        }
  //        String chunkName = basename+"_chunk"+i;
  //        ClientRequestsFileStorageInfo infoRequest =
  //            new ClientRequestsFileStorageInfo( chunkName );
  //        connection.sendData( infoRequest.getBytes() );
  //        reply = connection.receiveData();
  //        if ( reply == null ) {
  //          System.out.println(
  //              "\nThe Controller didn't respond to the info request for '"+
  //              chunkName+"'" );
  //          return;
  //        }
  //        ControllerSendsStorageList storageList =
  //            new ControllerSendsStorageList( reply );
  //        // Check which schema we are using...
  //        byte[] download = null;
  //        if ( storageType == 0 ) {
  //          download =
  //              getChunkFromReplicationServers( chunkName, storageList
  //              .servers,
  //                  connections );
  //        } else {
  //          byte[][] shards =
  //              getShardsFromServers( chunkName, storageList.servers,
  //                  connections );
  //          if ( shards != null ) {
  //            download = FileSynchronizer.getChunkFromShards( shards );
  //            download =
  //                FileSynchronizer.removeHashesFromChunk( download );
  //            download = FileSynchronizer.getDataFromChunk( download );
  //          }
  //        }
  //        if ( download == null ) {
  //          System.out.println(
  //              "\nCouldn't get '"+chunkName+"'. Stopping the download." );
  //          break;
  //        } else {
  //          // We have the data, now need to write the data to a file.
  //          FileSynchronizer.appendFile( location+basename, download );
  //          lastChunk++;
  //          if ( i == reportedSize.totalChunks-1 ) {
  //            finished = true;
  //            System.out.print( "100%\n" );
  //          }
  //        }
  //      }
  //      // Supposedly we are done writing the file to disk.
  //      if ( finished ) {
  //        System.out.println(
  //            "'"+basename+"' has been successfully saved to
  //            '"+location+"'" );
  //      } else {
  //        System.out.println(
  //            "'"+basename+"' was downloaded until chunk number
  //            "+lastChunk+"." );
  //        System.out.println(
  //            "It is stored on disk in the location specified, though "+
  //            "incomplete." );
  //      }
  //    } catch ( SocketTimeoutException ste ) {
  //      System.out.println( "Socket timed out: "+ste );
  //    } catch ( SocketException se ) {
  //      System.out.println( "Socket exception: "+se );
  //    } catch ( IOException ioe ) {
  //      System.out.println( "IOException: "+ioe );
  //    }
  //  }
  //
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

      case Protocol.CONTROLLER_SENDS_FILE_LIST:
        printFileList( event );
        break;

      case Protocol.CONTROLLER_RESERVES_SERVERS:
        notifyOfServers( event );
        break;

      case Protocol.CONTROLLER_DENIES_STORAGE_REQUEST:
        stopWriterAndRequestDelete( (( GeneralMessage ) event).getMessage() );
        break;

      default:
        System.err.println( "Event couldn't be processed. "+event.getType() );
        break;
    }
  }

  /**
   * Stops the ClientWriter writing the file with name 'filename', and sends a
   * delete request to the Controller for that particular file.
   *
   * @param filename filename to stop the writer for and request delete of
   */
  private synchronized void stopWriterAndRequestDelete(String filename) {
    System.out.println( "stopWriterAndRequestDelete" );
    ClientWriter writer = writers.get( filename );
    if ( writer != null ) {
      writer.setServersAndUnlock( null );
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
    System.out.println( "notifyOfServers" );
    for ( String server : message.getServers() ) {
      System.out.println( server );
    }
    ClientWriter writer = writers.get( message.getFilename() );
    if ( writer != null ) {
      writer.setServersAndUnlock( message.getServers() );
    }
    System.out.println( "notifyOfServers end" );
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

        case "delete":
          requestFileDelete( splitCommand );
          break;

        case "stop":
          stopHelper( splitCommand );
          break;

        case "writers":
          showWriters();
          break;

        case "list":
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
    // Need to close all TCPConnections, including controllerConnection
    controllerConnection.close();
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
      GeneralMessage deleteMessage =
          new GeneralMessage( Protocol.CLIENT_REQUESTS_FILE_DELETE,
              command[1] );
      try {
        controllerConnection.getSender().sendData( deleteMessage.getBytes() );
      } catch ( IOException ioe ) {
        System.err.println(
            "requestFileDelete: Couldn't send Controller a delete request. "+
            ioe.getMessage() );
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
      System.err.println( "That file already has an active writer. If the "+
                          "writer is frozen, interrupt it first, and try "+
                          "again." );
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
   * Prints a list of valid commands.
   */
  private void showHelp() {
    System.out.printf( "%3s%-19s : %s%n", "", "put path/local_file",
        "store a local file on the DFS" );
    System.out.printf( "%3s%-19s : %s%n", "", "delete filename",
        "request that a file be deleted from the DFS" );
    System.out.printf( "%3s%-19s : %s%n", "", "stop filename",
        "stops writer for 'filename' and sends delete request" );
    System.out.printf( "%3s%-19s : %s%n", "", "writers",
        "display list files in the process of being stored" );
    System.out.printf( "%3s%-19s : %s%n", "", "list",
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

  //
  //  /**
  //   * Receives commands from the user of the Client.
  //   */
  //  private void interact() {
  //    boolean exit = false;
  //    Scanner scanner = new Scanner( System.in );
  //    while ( !exit ) {
  //      System.out.print( "client> " );
  //      String command = scanner.nextLine();
  //      String[] splitCommand = command.split( "\\s+" );
  //      switch ( splitCommand[0].toLowerCase() ) {
  //        case "ls":
  //          ls();
  //          break;
  //        case "put":
  //          put( splitCommand );
  //          break;
  //        case "get":
  //          get( splitCommand );
  //          break;
  //        case "rm":
  //          rm( splitCommand );
  //          break;
  //        case "exit":
  //          closeSockets();
  //          exit = true;
  //          break;
  //        case "help":
  //          showHelp();
  //          break;
  //        default:
  //          System.err.println( "Unrecognized command. Use 'help' command." );
  //          break;
  //      }
  //    }
  //    scanner.close();
  //  }
  //
  //  /**
  //   * Calls delete() with the filename provided by the user.
  //   *
  //   * @param command
  //   */
  //  private void rm(String[] command) {
  //    if ( command.length < 2 ) {
  //      System.out.println( "Use the 'help' command for usage." );
  //      return;
  //    }
  //    delete( command[1] );
  //  }
  //
  //  /**
  //   * Calls retrieve() with the filename provided by the user.
  //   *
  //   * @param command
  //   */
  //  private void get(String[] command) {
  //    if ( command.length < 3 ) {
  //      System.out.println( "Use the 'help' command for usage." );
  //      return;
  //    }
  //    command[2] =
  //        command[2].replaceFirst( "^~", System.getProperty( "user.home" ) );
  //    File file = new File( command[2] );
  //    if ( file.isDirectory() ) {
  //      System.out.println(
  //          "Attempting to retrieve '"+command[1]+"' to save into
  //          '"+command[2]+
  //          "'" );
  //      retrieve( command[1], command[2] );
  //    } else {
  //      System.out.println( "'"+command[2]+"' is not a valid directory." );
  //    }
  //  }
  //
  //  /**
  //   * Calls store() for with the filename provided by the user.
  //   *
  //   * @param command
  //   */
  //  private void put(String[] command) {
  //    if ( command.length < 2 ) {
  //      System.out.println( "Use the 'help' command for usage." );
  //      return;
  //    }
  //    command[1] =
  //        command[1].replaceFirst( "^~", System.getProperty( "user.home" ) );
  //    File file = new File( command[1] );
  //    if ( file.isFile() ) {
  //      System.out.println( "Attempting to store '"+command[1]+"'" );
  //      store( storageType, command[1], connections );
  //    } else {
  //      System.out.println( "'"+command[1]+"' is not a valid file." );
  //    }
  //  }
  //
  //  /**
  //   * Print list of files stored in the DFS by asking the Controller.
  //   */
  //  private void ls() {
  //    String[] files = listFiles();
  //    for ( int i = 0; i < files.length; ++i ) {
  //      System.out.printf( "%3s%s%n", "", files[i] );
  //    }
  //  }
  //
  //  /**
  //   * Print a list of valid commands for the user.
  //   */
  //  private void showHelp() {
  //    System.out.printf( "%3s%-26s : %s%n", "", "ls",
  //        "print a list all files stored in the DFS" );
  //    System.out.printf( "%3s%-26s : %s%n", "", "put [file_path]",
  //        "store a file in the DFS" );
  //    System.out.printf( "%3s%-26s : %s%n", "", "get [filename] [save_path]",
  //        "retrieve a file from the DFS and save it locally" );
  //    System.out.printf( "%3s%-26s : %s%n", "", "rm [filename]",
  //        "delete a file from the DFS" );
  //    System.out.printf( "%3s%-26s : %s%n", "", "exit", "shutdown the
  //    client" );
  //    System.out.printf( "%3s%-26s : %s%n", "", "help",
  //        "print a list of valid commands" );
  //  }
  //
  //  /**
  //   * Closes all open sockets in TCPSenders to prepare for shutdown.
  //   */
  //  private void closeSockets() {
  //    Collection<TCPSender> values = connections.values();
  //    for ( TCPSender sender : values ) {
  //      sender.dout.close();
  //    }
  //  }
}