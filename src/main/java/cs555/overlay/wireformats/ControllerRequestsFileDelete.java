package cs555.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerRequestsFileDelete implements Event {

  public byte type;
  public String filename;

  public ControllerRequestsFileDelete(String filename) {
    this.filename = filename;
  }

  public ControllerRequestsFileDelete(byte[] msg) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap( msg );
    buffer.position( 1 );
    int length = buffer.getInt();
    byte[] array = new byte[length];
    buffer.get( array );
    this.filename = new String( array );
  }

  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( baOutputStream ) );
    dout.writeByte( Protocol.CONTROLLER_REQUESTS_FILE_DELETE );
    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );
    dout.flush();
    marshalledBytes = baOutputStream.toByteArray();
    baOutputStream.close();
    dout.close();
    baOutputStream = null;
    dout = null;
    return marshalledBytes;
  }

  public byte getType() {
    return Protocol.CONTROLLER_REQUESTS_FILE_DELETE;
  }
}