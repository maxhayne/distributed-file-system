package cs555.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ClientRequestsFileSize implements Event {

  public String filename;

  public ClientRequestsFileSize(String filename) {
    this.filename = filename;
  }

  public ClientRequestsFileSize(byte[] msg) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap( msg );
    buffer.position( 1 );
    int fileLength = buffer.getInt();
    byte[] filearray = new byte[fileLength];
    buffer.get( filearray );
    this.filename = new String( filearray );
  }

  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( baOutputStream ) );

    dout.writeByte( Protocol.CLIENT_REQUESTS_FILE_SIZE );
    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

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
    return Protocol.CLIENT_REQUESTS_FILE_SIZE;
  }
}