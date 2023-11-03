package cs555.overlay.util;

import cs555.overlay.node.Client;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.wireformats.ClientStore;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.SendsFileForStorage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class used by the Client to read a file from disk and send (write) its
 * chunks to the DFS.
 *
 * @author hayne
 */
public class ClientWriter implements Runnable {

  private final Client client;
  private final Path pathToFile;
  private final AtomicInteger chunksSent;
  private final AtomicInteger totalChunks;
  private final TCPConnectionCache connectionCache;
  private String[] servers;

  /**
   * Constructor. Creates a new ClientWriter which will be ready to be passed to
   * a new thread to read the file stored at the location specified by
   * 'pathToFile'.
   *
   * @param client Client on which the ClientWriter will be executing
   * @param pathToFile full path of file to be stored on the DFS
   */
  public ClientWriter(Client client, Path pathToFile) {
    this.client = client;
    this.pathToFile = pathToFile;
    this.chunksSent = new AtomicInteger( 0 );
    this.totalChunks = new AtomicInteger( 1 );
    this.connectionCache = new TCPConnectionCache();
  }

  /**
   * Sets 'servers' member, and notifies the waiting thread (in the run method)
   * that it should try to send the next chunk to the provided servers.
   *
   * @param servers String[] of host:port addresses to servers provided by the
   * Controller.
   */
  public synchronized void setServersAndNotify(String[] servers) {
    this.servers = servers;
    this.notify();
  }

  /**
   * The ClientWriter's working method. Opens the file pointed to by the path
   * given in the constructor and reads one chunk at a time. After reading a new
   * chunk, it asks the Controller for a list of servers to store the chunk and
   * waits for a response. Upon receiving a response, it attempts to send the
   * chunk to one of the servers in the list. This process is repeated for the
   * entire file, unless a problems occurs, in which case the storage operation
   * is cancelled.
   */
  @Override
  public synchronized void run() {
    try ( RandomAccessFile file = new RandomAccessFile( pathToFile.toString(),
        "r" ); FileChannel channel = file.getChannel();
          FileLock fileLock = channel.lock( 0, file.length(), true ) ) {
      chunkizeFileAndStore( file, setTotalChunks( file.length() ) );
    } catch ( IOException|InterruptedException ioe ) {
      System.err.println( "ClientWriter: Exception thrown while writing '"+
                          pathToFile.toString()+"' to the DFS. "+
                          ioe.getMessage() );
    }
    try {
      cleanup();
    } catch ( InterruptedException ie ) {
      System.err.println( pathToFile.getFileName()+" cleanup() interrupted." );
    }
  }

  /**
   * Attempts to read the file into chunks, request servers to store those
   * chunks from the Controller, and send those chunks to those servers.
   *
   * @param file RandomAccessFile that has been opened for the file being
   * read (assuming an exclusive lock has already been acquired)
   * @param totalChunks the total number of chunks that should be read from
   * the file
   * @throws IOException if the function encounters a problem while reading
   * the file
   * @throws InterruptedException if the function is interrupted while
   * waiting for servers from the Controller
   */
  private void chunkizeFileAndStore(RandomAccessFile file, int totalChunks)
      throws IOException, InterruptedException {
    ClientStore requestMessage = createNewStoreMessage(); // reusable
    byte[] chunk = new byte[FileSynchronizer.CHUNK_DATA_LENGTH]; // reusable
    for ( int i = 0; i < totalChunks; ++i ) {
      byte[] chunkContent = readAndResize( file, chunk );
      if ( chunkContent != null && sendToController( requestMessage ) ) {
        this.wait(); // wait for Controller to send allocated servers
        if ( servers == null ||
             !sendChunkToServers( requestMessage.getSequence(),
                 chunkContent ) ) { // stopped by user, or chunk not sent out
          break;
        }
      } else {
        break;
      }
      chunksSent.incrementAndGet();
      requestMessage.incrementSequence(); // set sequence for next chunk
    }
  }

  /**
   * Cleans up this ClientReader.
   */
  private void cleanup() throws InterruptedException {
    client.removeWriter( pathToFile.getFileName().toString() ); // remove self
    Thread.sleep( 1000 );
    connectionCache.closeConnections(); // shutdown connections
    System.out.println(
        "The ClientWriter for '"+pathToFile.getFileName()+"' has cleaned up." );
  }

  /**
   * Reads a file's contents (from the current position) into a byte[], and
   * returns a resized array if fewer bytes than the size of the array were
   * read.
   *
   * @param file file to be read
   * @param chunk byte[] to fill with file's bytes
   * @return null if the read failed, byte[] of data otherwise
   * @throws IOException if an exception is thrown while reading the file
   */
  private byte[] readAndResize(RandomAccessFile file, byte[] chunk)
      throws IOException {
    Arrays.fill( chunk, ( byte ) 0 );
    int bytesRead = file.read( chunk ); // doesn't read fully
    if ( bytesRead == -1 ) {
      return null;
    }
    return bytesRead > 0 && bytesRead < 65536 ?
               Arrays.copyOfRange( chunk, 0, bytesRead ) : chunk;
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
   * Attempts to send the chunk to one of the servers in the 'servers' array. If
   * sending to one of them fails, tries to send to the others.
   *
   * @param sequence of the chunk being sent
   * @param content of the chunk being sent
   * @return true if sent to one of the servers, false if the chunk couldn't be
   * sent
   */
  private boolean sendChunkToServers(int sequence, byte[] content) {
    byte[][] contentToSend = createContentToSend( content );
    SendsFileForStorage sendMessage =
        new SendsFileForStorage( createFilename( sequence ), contentToSend,
            servers );
    boolean sent;
    do {
      sent = sendToChunkServer( sendMessage, sendMessage.getServer() );
    } while ( !sent && sendMessage.nextPosition() );
    return sent;
  }

  /**
   * Attempts to send a message to a server with address 'address'.
   *
   * @param event message to be sent
   * @param address of server to send message to
   * @return true if sent, false if not
   */
  private boolean sendToChunkServer(Event event, String address) {
    try {
      connectionCache.getConnection( client, address, false )
                     .getSender()
                     .sendData( event.getBytes() );
      return true;
    } catch ( IOException ioe ) {
      System.err.println(
          "sendToChunkServer: Couldn't send file to '"+address+"'. " );
      return false;
    }
  }

  /**
   * Transforms the content read from disk to the right format to send to the
   * servers, but the process is different depending on whether we're
   * replicating or erasure coding.
   *
   * @param content of chunk read from the disk
   * @return byte[][] of content to be attached to the SendsFileForStorage
   * message
   */
  private byte[][] createContentToSend(byte[] content) {
    if ( client.getStorageType() == 0 ) { // replicating
      return new byte[][]{ content };
    } else { // erasure coding
      int length = content.length;
      content = standardizeLength( content );
      return FileSynchronizer.makeShardsFromContent( length, content );
    }
  }

  /**
   * Takes a byte[] of length<Constants.CHUNK_DATA_LENGTH and returns a byte[]
   * of length Constants.CHUNK_DATA_LENGTH with the original content copied into
   * the new buffer.
   *
   * @param content to standardize
   * @return byte[] of length Constants.CHUNK_DATA_LENGTH with copied content
   */
  private byte[] standardizeLength(byte[] content) {
    if ( content.length == Constants.CHUNK_DATA_LENGTH ) {
      return content;
    } else {
      byte[] buf = new byte[Constants.CHUNK_DATA_LENGTH];
      System.arraycopy( content, 0, buf, 0, content.length );
      return buf;
    }
  }

  /**
   * Calculates the number of chunks to read based on the size of the file in
   * bytes, and sets member 'totalChunks' to that value.
   *
   * @param fileSize in bytes
   * @return number of chunks to read
   */
  private int setTotalChunks(long fileSize) {
    int total = ( int ) Math.ceil( ( double ) fileSize/( double ) 65536 );
    totalChunks.set( total );
    return total;
  }

  /**
   * Returns a number between 0 and 100 representing the progress in storing the
   * chunks of the file being written to the DFS.
   *
   * @return percentage of file written to DFS
   */
  public int getProgress() {
    return ( int ) ((( double ) chunksSent.get()/( double ) totalChunks.get())*
                    100.0);
  }

  /**
   * Getter for 'pathToFile'.
   *
   * @return pathToFile
   */
  public Path getPathToFile() {
    return pathToFile;
  }

  /**
   * Creates the initial message type based on the storageType of the Client.
   *
   * @return new ClientStore message with correct filename, and sequence number
   * of 0
   */
  private ClientStore createNewStoreMessage() {
    String filename = pathToFile.getFileName().toString();
    return new ClientStore( filename, 0 );
  }

  /**
   * Creates proper filename for the SendsFileForStorage message, which is
   * different if we're using erasure coding or replication.
   *
   * @param sequence of chunk to be stored
   * @return filename
   */
  private String createFilename(int sequence) {
    String filename = pathToFile.getFileName().toString()+"_chunk"+sequence;
    if ( client.getStorageType() == 1 ) {
      filename += "_shard";
    }
    return filename;
  }
}