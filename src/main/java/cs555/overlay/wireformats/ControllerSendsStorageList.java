package cs555.overlay.wireformats;

import java.io.*;

public class ControllerSendsStorageList implements Event {

  private final byte type;
  private final String filename;
  private final String[] servers;

  public ControllerSendsStorageList(String filename, String[] servers) {
    this.type = Protocol.CONTROLLER_SENDS_STORAGE_LIST;
    this.filename = filename;
    this.servers = servers;
  }

  public ControllerSendsStorageList(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    int serverCount = din.readInt();
    if ( serverCount != 0 ) {
      servers = new String[serverCount];
      for ( int i = 0; i < serverCount; ++i ) {
        len = din.readInt();
        array = new byte[len];
        din.readFully( array );
        servers[i] = new String( array );
      }
    } else {
      servers = null;
    }

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    // Write list of servers
    int serverCount = servers != null ? servers.length : 0;
    dout.writeInt( serverCount );
    for ( int i = 0; i < serverCount; ++i ) {
      array = servers[i].getBytes();
      dout.writeInt( array.length );
      dout.write( array );
    }

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
