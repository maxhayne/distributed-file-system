package cs555.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ClientRequestsStoreShards implements Event {

  public String filename;
  public int sequence;

  public ClientRequestsStoreShards(String filename, int sequence) {
    this.filename = filename;
    this.sequence = sequence;
  }

  public ClientRequestsStoreShards(byte[] msg) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap( msg );
    buffer.position( 1 );
    int length = buffer.getInt();
    byte[] array = new byte[length];
    buffer.get( array );
    this.filename = new String( array );
    this.sequence = buffer.getInt();
  }

  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( baOutputStream ) );

    dout.writeByte( Protocol.CLIENT_REQUESTS_STORE_SHARDS );
    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );
    dout.writeInt( sequence );

    dout.flush();
    marshalledBytes = baOutputStream.toByteArray();
    baOutputStream.close();
    dout.close();
    baOutputStream = null;
    dout = null;
    return marshalledBytes;
  }

  public byte getType() {
    return Protocol.CLIENT_REQUESTS_STORE_SHARDS;
  }
}