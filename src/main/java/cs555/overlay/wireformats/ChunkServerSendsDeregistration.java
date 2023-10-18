package cs555.overlay.wireformats;

import java.io.*;

public class ChunkServerSendsDeregistration implements Event {

  public byte type;
  public int identifier;

  public ChunkServerSendsDeregistration(int identifier) {
    this.type = Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION;
    this.identifier = identifier;
  }

  public ChunkServerSendsDeregistration(byte[] marshalledBytes)
      throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    identifier = din.readInt();

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    dout.writeInt( identifier );

    byte[] returnable = bout.toByteArray();
    dout.close();
    bout.close();
    return returnable;
  }

  @Override
  public byte getType() {
    return type;
  }
}