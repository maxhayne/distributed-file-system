package cs555.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ClientRequestsFileList implements Event {

  public ClientRequestsFileList() {
    // Empty constructor
  }

  public ClientRequestsFileList(byte[] msg) {
    // Empty decoder
  }

  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( baOutputStream ) );
    dout.writeByte( Protocol.CLIENT_REQUESTS_FILE_LIST );
    dout.flush();
    marshalledBytes = baOutputStream.toByteArray();
    baOutputStream.close();
    dout.close();
    baOutputStream = null;
    dout = null;
    return marshalledBytes;
  }

  public byte getType() {
    return Protocol.CLIENT_REQUESTS_FILE_LIST;
  }
}