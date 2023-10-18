package cs555.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ControllerSendsHeartbeat implements Event {

  public ControllerSendsHeartbeat() {
    // Empty constructor
  }

  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( baOutputStream ) );
    dout.writeByte( Protocol.CONTROLLER_SENDS_HEARTBEAT );
    dout.flush();
    marshalledBytes = baOutputStream.toByteArray();
    baOutputStream.close();
    dout.close();
    baOutputStream = null;
    dout = null;
    return marshalledBytes;
  }

  public byte getType() {
    return Protocol.CONTROLLER_SENDS_HEARTBEAT;
  }
}