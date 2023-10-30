package cs555.overlay.wireformats;

import java.io.*;

public class ChunkServerNoStoreFile implements Event {

  private final byte type;
  private final String address;
  // Controller performs lookup to find the identifier of server that
  // failed to store the file
  private final String filename;

  public ChunkServerNoStoreFile(String address, String filename) {
    this.type = Protocol.CHUNK_SERVER_NO_STORE_FILE;
    this.address = address;
    this.filename = filename;
  }

  public ChunkServerNoStoreFile(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    address = new String( array );

    len = din.readInt();
    array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    byte[] array = address.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

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
   * Getter for filename.
   *
   * @return filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Getter for address.
   *
   * @return host:port address
   */
  public String getAddress() {
    return address;
  }
}