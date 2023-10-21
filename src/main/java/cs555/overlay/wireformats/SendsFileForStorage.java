package cs555.overlay.wireformats;

import java.io.*;

public class SendsFileForStorage implements Event {

  private final byte type;
  private final String filename;
  private final byte[] content;
  private String[] servers;
  private int position;

  public SendsFileForStorage(String filename, byte[] content,
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

    len = din.readInt();
    array = new byte[len];
    din.readFully( array );
    content = array;

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
    dout.write( content );

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

    byte[] returnable = bout.toByteArray();
    dout.close();
    bout.close();
    return returnable;
  }

  @Override
  public byte getType() {
    return type;
  }

  /**
   * Getter for filename to be stored.
   *
   * @return filename string
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Increment 'position' member to point to the index of the next server in
   * relay list.
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
   * Returns the file's content.
   *
   * @return byte[] of file's content
   */
  public byte[] getContent() {
    return content;
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