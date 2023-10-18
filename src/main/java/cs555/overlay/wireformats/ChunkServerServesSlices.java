package cs555.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerServesSlices implements Event {

  public String filename;
  public int[] slices;
  public byte[][] slicedata;

  public ChunkServerServesSlices(String filename, int[] slices,
      byte[][] slicedata) {
    this.filename = filename;
    this.slices = slices;
    this.slicedata = slicedata;
  }

  public ChunkServerServesSlices(byte[] msg) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap( msg );
    buffer.position( 1 );
    int fileLength = buffer.getInt();
    byte[] filearray = new byte[fileLength];
    buffer.get( filearray );
    this.filename = new String( filearray );
    int numSlices = buffer.getInt();
    this.slices = new int[numSlices];
    this.slicedata = new byte[numSlices][8195];
    for ( int i = 0; i < numSlices; i++ ) {
      this.slices[i] = buffer.getInt();
      buffer.get( slicedata[i] );
    }
  }

  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( baOutputStream ) );

    dout.writeByte( Protocol.CHUNK_SERVER_SERVES_SLICES );
    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );
    int numSlices = slices.length;
    dout.writeInt( numSlices );
    for ( int i = 0; i < numSlices; i++ ) {
      dout.writeInt( slices[i] );
      dout.write( slicedata[i] );
    }
    dout.flush();
    marshalledBytes = baOutputStream.toByteArray();

    baOutputStream.close();
    dout.close();
    baOutputStream = null;
    dout = null;
    array = null;
    return marshalledBytes;
  }

  public byte getType() {
    return Protocol.CHUNK_SERVER_SERVES_SLICES;
  }
}