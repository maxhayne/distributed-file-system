package cs555.overlay.wireformats;

import java.io.*;

public class ControllerRequestsFileAcquire implements Event {

  public byte type;
  public String filename;
  public String[] servers;

  public ControllerRequestsFileAcquire(String filename, String[] servers) {
    this.type = Protocol.CONTROLLER_REQUESTS_FILE_ACQUIRE;
    this.filename = filename;
    this.servers = servers;
  }

  public ControllerRequestsFileAcquire(byte[] marshalledBytes)
      throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    int serverLength = din.readInt();
    servers = new String[serverLength];
    for ( int i = 0; i < serverLength; ++i ) {
      len = din.readInt();
      array = new byte[len];
      din.readFully( array );
      servers[i] = new String( array );
    }

    din.close();
    bin.close();
  }

  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    if ( servers != null ) {
      dout.writeInt( servers.length );
      for ( int i = 0; i < servers.length; ++i ) {
        array = servers[i].getBytes();
        dout.writeInt( array.length );
        dout.write( array );
      }
    } else {
      dout.writeInt( 0 );
    }

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }

  public byte getType() {
    return type;
  }
}