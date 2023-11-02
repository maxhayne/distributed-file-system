package cs555.overlay.wireformats;

import java.io.*;

public class ClientStore implements Event {
  private final byte type;
  private final String filename;
  private int sequence;

  public ClientStore(String filename, int sequence) {
    this.type = Protocol.CLIENT_STORE;
    this.filename = filename;
    this.sequence = sequence;
  }

  public ClientStore(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    sequence = din.readInt();

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.writeByte( type );

    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    dout.writeInt( sequence );

    byte[] marshalledBytes = bout.toByteArray();
    bout.close();
    dout.close();
    return marshalledBytes;
  }

  @Override
  public byte getType() {
    return type;
  }

  public String getFilename() {
    return filename;
  }

  public int getSequence() {
    return sequence;
  }

  public void incrementSequence() {
    sequence++;
  }
}
