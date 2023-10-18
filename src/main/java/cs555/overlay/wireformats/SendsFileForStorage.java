package cs555.overlay.wireformats;

import java.io.*;

public class SendsFileForStorage implements Event {

  public byte type;
  public String filename;
  public byte[] data;
  public String[] servers;
  public int nextServer;

  public SendsFileForStorage(String filename, byte[] data, String[] servers) {
    this.type = Protocol.SENDS_FILE_FOR_STORAGE;
    this.filename = filename;
    this.data = data;
    this.servers = servers;
    this.nextServer = 0;
  }

  public SendsFileForStorage(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    len = din.readInt();
    array = new byte[len];
    din.readFully( array );
    data = array;

    int numServers = din.readInt();
    if ( numServers != 0 ) {
      servers = new String[numServers];
      for ( int i = 0; i < numServers; ++i ) {
        len = din.readInt();
        array = new byte[len];
        din.readFully( array );
        servers[i] = new String( array );
      }
    }

    nextServer = din.readInt();

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

    dout.writeInt( data.length );
    dout.write( data );

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

    dout.writeInt( nextServer );

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