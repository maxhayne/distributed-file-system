package cs555.overlay.util;

import cs555.overlay.node.Client;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.wireformats.ClientStore;
import cs555.overlay.wireformats.Protocol;
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
  private final Object lock;
  private String[] servers;

  public ClientWriter(Client client, Path pathToFile) {
    this.client = client;
    this.pathToFile = pathToFile;
    this.chunksSent = new AtomicInteger( 0 );
    this.totalChunks = new AtomicInteger( 1 );
    this.connectionCache = new TCPConnectionCache();
    this.lock = new Object();
  }

  public void setServersAndUnlock(String[] servers) {
    System.out.println( "setServersAndUnlock" );
    this.servers = servers;
    unlock();
    System.out.println( "setServersAndUnlock end" );
  }

  private void unlock() {
    synchronized( lock ) {
      lock.notify();
      System.out.println( "notified" );
    }
  }

  @Override
  public void run() {
    try ( RandomAccessFile file = new RandomAccessFile( pathToFile.toString(),
        "r" ); FileChannel channel = file.getChannel();
          FileLock fileLock = channel.lock( 0, file.length(), true ) ) {
      System.out.println( "The file has been accessed!" );
      totalChunks.set( getTotalChunks( file.length() ) );
      int counter = totalChunks.get();
      byte[] chunk = new byte[65536];
      ClientStore requestMessage = createNewStoreMessage();
      long before = System.currentTimeMillis();
      for ( int i = 0; i < counter; ++i ) {
        Arrays.fill( chunk, ( byte ) 0 );
        int bytesRead = file.read( chunk ); // doesn't read fully
        byte[] content = chunk;
        if ( bytesRead == -1 ) {
          break;
        } else if ( bytesRead > 0 && bytesRead < 65536 ) {
          content = Arrays.copyOfRange( chunk, 0, bytesRead );
        }
        try {
          client.getControllerConnection()
                .getSender()
                .sendData( requestMessage.getBytes() );
        } catch ( IOException ioe ) {
          System.err.println( "Couldn't send request to Controller, halting." );
          break;
        }
        synchronized( lock ) {
          lock.wait();
        }
        // servers have been set at this point
        boolean sent =
            sendChunkToServers( requestMessage.getSequence(), content );
        if ( !sent ) {
          System.err.println( "ClientWriter::run: Storage message couldn't "+
                              "be sent to the first ChunkServer. " );
          break;
        } else {
          chunksSent.incrementAndGet();
          System.out.println( "ClientWriter::run: Storage message sent. " );
        }
        // increment sequence for next message to controller
        requestMessage.incrementSequence();
      }
      System.out.println( (System.currentTimeMillis()-before)+"ms to read "+
                          file.length()/(1024*1024)+"MB" );
    } catch ( IOException|InterruptedException ioe ) {
      System.err.println( "ClientWriter::run: File couldn't be read." );
    } finally {
      // When we are done, remove self from writers map?
      client.removeWriter( pathToFile.getFileName().toString() );
      // Should clear the TCPConnectionCache
      System.out.println( pathToFile+" has finished writing." );
    }
  }

  private boolean sendChunkToServers(int sequence, byte[] content) {
    byte[][] contentToSend = createContentToSend( content );
    SendsFileForStorage sendMessage =
        new SendsFileForStorage( createFilename( sequence ), contentToSend,
            servers );
    try {
      connectionCache.getConnection( client, servers[0], false )
                     .getSender()
                     .sendData( sendMessage.getBytes() );
      return true;
    } catch ( IOException ioe ) {
      System.err.println(
          "sendToChunkServers: Couldn't send file to first ChunkServer. "+
          ioe.getMessage() );
      return false;
    }
  }

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
   * bytes.
   *
   * @param fileSize in bytes
   * @return number of chunks to read
   */
  private int getTotalChunks(long fileSize) {
    return ( int ) Math.ceil( ( double ) fileSize/( double ) 65536 );
  }

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
    return client.getStorageType() == 0 ?
               new ClientStore( Protocol.CLIENT_STORE_CHUNK, filename, 0 ) :
               new ClientStore( Protocol.CLIENT_STORE_SHARDS, filename, 0 );
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