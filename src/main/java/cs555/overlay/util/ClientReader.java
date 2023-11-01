package cs555.overlay.util;

import cs555.overlay.node.Client;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.GeneralMessage;
import cs555.overlay.wireformats.Protocol;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientReader implements Runnable {

  private final Client client;
  private final String filename;
  private final AtomicInteger chunksReceived;
  private final AtomicInteger totalChunks;
  private final TCPConnectionCache connectionCache;
  private final Path readDirectory;

  private String[][] servers = null; // set by Controller response
  private byte[][][] receivedFiles = null; // set by Controller response

  private CountDownLatch writeLatch; // prevents writing to disk until ready

  public ClientReader(Client client, String filename) {
    this.client = client;
    this.filename = filename;
    this.chunksReceived = new AtomicInteger( 0 );
    this.totalChunks = new AtomicInteger( 0 );
    this.connectionCache = new TCPConnectionCache();
    this.readDirectory = Paths.get( System.getProperty( "user.dir" ), "reads" );
  }

  @Override
  public synchronized void run() {
    if ( createReadDirectory() ) {
      try ( RandomAccessFile file = new RandomAccessFile(
          readDirectory.resolve( filename ).toString(), "rw" );
            FileChannel channel = file.getChannel();
            FileLock fileLock = channel.lock() ) {
        if ( getStorageInfo() ) { // We've retrieved the storage info
          initializeReceivedFiles();
          setTotalChunks();
          createLatch();
          wrangleChunks();
          channel.truncate( 0 ); // truncate the file if it exists
          writeChunksToDisk( file );
        }
      } catch ( IOException|InterruptedException ioe ) {
        System.err.println(
            "ClientReader: Exception thrown while writing '"+filename+
            "' to disk. "+ioe.getMessage() );
      }
    }
    try {
      cleanup();
    } catch ( InterruptedException ie ) {
      System.err.println( filename+" cleanup() interrupted." );
    }
  }

  /**
   * Creates a latch based on the total number of chunks we must gather.
   */
  private void createLatch() {
    writeLatch = new CountDownLatch( totalChunks.get() );
  }

  /**
   * Creates total chunks based on the number of available slots in
   * receivedFiles.
   */
  private void setTotalChunks() {
    totalChunks.set( receivedFiles.length );
  }

  /**
   * Creates a new byte[][][] receivedFiles based on the length of the
   */
  private void initializeReceivedFiles() {
    receivedFiles =
        client.getStorageType() == 0 ? new byte[servers.length][1][] :
            new byte[servers.length][Constants.TOTAL_SHARDS][];
  }

  public void addFile(String filename, byte[] content) {
    int sequence = FilenameUtilities.getSequence( filename );
    int fragment = client.getStorageType() == 1 ?
                       FilenameUtilities.getFragment( filename ) : 0;
    synchronized( receivedFiles[sequence] ) {
      if ( client.getStorageType() == 0 ) { // replication
        receivedFiles[sequence][0] = content;
        writeLatch.countDown();
        chunksReceived.incrementAndGet();
      } else { // erasure
        receivedFiles[sequence][fragment] = content;
        if ( ArrayUtilities.countNulls( receivedFiles[sequence] ) == 0 ) {
          writeLatch.countDown();
          chunksReceived.incrementAndGet();
        }
      }
    }
  }

  private void writeChunksToDisk(RandomAccessFile file) throws IOException {
    for ( int i = 0; i < receivedFiles.length; ++i ) {
      synchronized( receivedFiles[i] ) {
        if ( client.getStorageType() == 0 ) { // for replication
          if ( receivedFiles[i][0] != null ) {
            file.write( receivedFiles[i][0] );
          } else {
            System.out.println( "receivedFiles["+i+"] is null" );
          }
        } else { // for erasure coding
          byte[][] decoded =
              FileSynchronizer.decodeMissingShards( receivedFiles[i] );
          if ( decoded != null ) {
            byte[] content = FileSynchronizer.getContentFromShards( decoded );
            file.write( content );
          }
        }
      }
    }
  }

  /**
   * Sends file request messages the minimum number of servers that need to be
   * contacted to reconstruct the file. In the case of erasure coding, sends
   * file requests to Constants.TOTAL_SHARDS servers that might hold fragments
   * (even though only Constants.DATA_SHARDS fragments are needed). In the case
   * of replication, sends out one request for each chunk for every iteration of
   * the loop. When all servers have been contacted, or all servers that have
   * been contacted have responded (whichever comes first), the function
   * returns.
   *
   * @throws InterruptedException if wrangleLatch.await() is interrupted for
   * some reason
   */
  private void wrangleChunks() throws InterruptedException {
    int requests;
    do {
      System.out.println( "wrangling" );
      requests = requestUnaskedServers( true );
      if ( requests == 0 ) {
        System.out.println( "requests == 0" );
        break; // stop the process if no other servers can be contacted
      } else {
        requestUnaskedServers( false );
      }
    } while ( !writeLatch.await( 10000+(10L*requests),
        TimeUnit.MILLISECONDS ) );
    System.out.println( writeLatch.getCount() );
  }

  /**
   * Sends file request messages to servers that haven't been asked yet.
   *
   * @param dryRun if true, the function doesn't actually send messages to the
   * servers, just counts the total number of times it would have
   * @return total number of servers asked
   */
  private int requestUnaskedServers(boolean dryRun) {
    // Create general purpose message
    GeneralMessage requestMessage =
        new GeneralMessage( Protocol.REQUEST_FILE, "" );
    int askCount = 0;
    for ( int i = 0; i < servers.length; ++i ) {
      for ( int j = 0; j < servers[i].length; ++j ) {
        if ( servers[i][j] != null ) {
          if ( !dryRun ) {
            requestFileFromServer( servers[i][j], i, j, requestMessage );
            servers[i][j] = null;
          }
          askCount++;
          if ( client.getStorageType() == 0 ) { // only ask one if replicating
            break;
          }
        }
      }
    }
    return askCount;
  }

  /**
   * Requests a file from a server using information given in the parameters.
   *
   * @param address of server to request the file from
   * @param sequence number of chunk
   * @param serverPosition position of address in server array (could correspond
   * to fragment number if we're erasure coding)
   * @param requestMessage reusable request message of type Protocol
   * .REQUEST_FILE
   * @return true if request message was sent to server, false otherwise
   */
  private boolean requestFileFromServer(String address, int sequence,
      int serverPosition, GeneralMessage requestMessage) {
    String specificFilename = appendFilename( sequence, serverPosition );
    requestMessage.setMessage( specificFilename );
    try {
      connectionCache.getConnection( client, address, true )
                     .getSender()
                     .sendData( requestMessage.getBytes() );
      return true; // message sent
    } catch ( IOException ioe ) {
      System.err.println( "requestFileFromServer: "+specificFilename+" "+
                          "could not be requested from "+address+". "+
                          ioe.getMessage() );
      return false; // message not sent
    }
  }

  /**
   * Creates the filename for a file request from a server based on the sequence
   * number and server position. If the client is erasure coding, the
   * serverPosition is relevant, and corresponds to the fragment number.
   * Otherwise, it is ignored.
   *
   * @param sequence number of chunk
   * @param serverPosition of server being requested
   * @return filename of chunk/shard to be requested
   */
  private String appendFilename(int sequence, int serverPosition) {
    return client.getStorageType() == 0 ? filename+"_chunk"+sequence :
               filename+"_chunk"+sequence+"_shard"+serverPosition;
  }

  /**
   * Cleans up this ClientReader.
   */
  private void cleanup() throws InterruptedException {
    client.removeReader( filename ); // remove self
    Thread.sleep( 1000 );
    connectionCache.closeConnections(); // shutdown connections
    System.out.println( "The ClientReader for '"+filename+"' has closed." );
  }

  /**
   * Create the directory into which the file will be stored.
   *
   * @return true if directory created, false otherwise
   */
  private boolean createReadDirectory() {
    try {
      Files.createDirectories( readDirectory );
      return true;
    } catch ( IOException e ) {
      System.err.println(
          "Couldn't create directory to store files read from the DFS: "+
          readDirectory );
      return false;
    }
  }

  /**
   * Name is self-explanatory. Will be called from the Client when the
   * Controller has responded to a file storage info request.
   *
   * @param servers what to set 'servers' to
   */
  public synchronized void setServersAndNotify(String[][] servers) {
    this.servers = servers;
    this.notify();
  }

  /**
   * Sends a message to the Controller asking for the storage information for
   * the filename given to this ClientReader in the constructor. Waits to be
   * unlocked, which will either happen when a response arrives from the
   * Controller, or when the user issues an interruption.
   *
   * @return the total number of chunks stored on the DFS,
   * @throws InterruptedException if the waiting thread is interrupted
   */
  private synchronized boolean getStorageInfo() throws InterruptedException {
    GeneralMessage storageInfoMessage =
        new GeneralMessage( Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO,
            filename );
    if ( sendToController( storageInfoMessage ) ) {
      this.wait(); // wait until Controller has sent storage info
    }
    if ( servers != null ) {
      return true;
    } else {
      System.err.println( "'"+filename+"' has zero chunks stored on the DFS." );
      return false;
    }
  }

  /**
   * Sends a message to the Controller.
   *
   * @param event message to send
   * @return true if sent, false if not
   */
  private boolean sendToController(Event event) {
    try {
      client.getControllerConnection().getSender().sendData( event.getBytes() );
      return true;
    } catch ( IOException ioe ) {
      System.err.println( "Couldn't send message to Controller." );
      return false;
    }
  }

  /**
   * Returns a number between 0 and 100 representing the progress in retrieving
   * the chunks of the file from the DFS.
   *
   * @return percentage of files retrieved from DFS
   */
  public int getProgress() {
    return totalChunks.get() == 0 ? 0 : ( int ) (
        (( double ) chunksReceived.get()/( double ) totalChunks.get())*100.0);
  }
}