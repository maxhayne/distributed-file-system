package cs555.overlay.wireformats;

import java.io.*;

/**
 * Sent by the Controller to a ChunkServer, carrying the content of a chunk to
 * be written to disk. Message will be forwarded to other ChunkServers that must
 * store the same chunk.
 *
 * @author hayne
 */
public class SendsFileForStorage implements Event {
  private final byte type;
  private final String filename;
  private final byte[][] content;
  private String[] servers;
  private int position;

  /**
   * Constructor. If the file to be stored is being stored using the replication
   * schema, filename should be 'filename_chunk#'. If using the erasure coding
   * schema, filename should be 'filename_chunk#_shard' (without the fragment
   * number).
   *
   * @param filename of file to be stored
   * @param content of file to be stored
   * @param servers to store the file at
   */
  public SendsFileForStorage(String filename, byte[][] content,
      String[] servers) {
    this.type = Protocol.SENDS_FILE_FOR_STORAGE;
    this.filename = filename;
    this.content = content;
    this.servers = servers;
    this.position = 0;
  }

  public SendsFileForStorage(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    int numArrays = din.readInt();
    content = new byte[numArrays][];
    for ( int i = 0; i < numArrays; ++i ) {
      len = din.readInt();
      if ( len != 0 ) {
        content[i] = new byte[len];
        din.readFully( content[i] );
      }
    }

    int numServers = din.readInt();
    if ( numServers != 0 ) {
      servers = new String[numServers];
      for ( int i = 0; i < numServers; ++i ) {
        len = din.readInt();
        array = new byte[len];
        din.readFully( array );
        servers[i] = new String( array );
      }
    }

    position = din.readInt();

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    dout.writeInt( content.length );
    for ( byte[] data : content ) {
      if ( data == null ) {
        dout.writeInt( 0 );
      } else {
        dout.writeInt( data.length );
        dout.write( data );
      }
    }

    if ( servers != null ) {
      dout.writeInt( servers.length );
      for ( String server : servers ) {
        array = server.getBytes();
        dout.writeInt( array.length );
        dout.write( array );
      }
    } else {
      dout.writeInt( 0 );
    }

    dout.writeInt( position );

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }

  @Override
  public byte getType() {
    return type;
  }

  /**
   * Getter for filename to be stored. If the file being stored is a shard, the
   * filename will change from server to server, so 'position' will be appended
   * in that case.
   *
   * @return filename string
   */
  public String getFilename() {
    if ( filename.contains( "shard" ) ) {
      return filename+position;
    } else {
      return filename;
    }
  }

  /**
   * If we have visited 'servers.length' servers, returns false. If we haven't
   * yet, increments 'position' by one and returns true.
   *
   * @return true if there is another server to relay to, false if not
   */
  public boolean nextPosition() {
    if ( position < servers.length-1 ) {
      position++;
      return true;
    }
    return false;
  }

  /**
   * Returns the file's content. If using erasure coding, every server will
   * store a different fragment, but if we're replicating, every server will
   * store the same thing.
   *
   * @return byte[] of file's content
   */
  public byte[] getContent() {
    if ( filename.contains( "shard" ) ) {
      byte[] copy = new byte[content[position].length];
      System.arraycopy( content[position], 0, copy, 0, copy.length );
      content[position] = null; // So we don't relay it to the next ChunkServer
      return copy;
    } else {
      return content[0];
    }
  }

  /**
   * Returns the host:port address of the server in servers[position]. Should be
   * called if nextPosition() was just called and returned true.
   *
   * @return host:port address of servers[position]
   */
  public String getServer() {
    return servers[position];
  }
}