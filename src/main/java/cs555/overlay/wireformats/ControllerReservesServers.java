package cs555.overlay.wireformats;

import java.io.*;

public class ControllerReservesServers implements Event {
  private final byte type;
  private final String filename;
  private final int sequence;
  private final String[] servers;

  public ControllerReservesServers(String filename, int sequence,
      String[] servers) {
    this.type = Protocol.CONTROLLER_RESERVES_SERVERS;
    this.filename = filename;
    this.sequence = sequence;
    this.servers = servers;
  }

  public ControllerReservesServers(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    sequence = din.readInt();

    int numberOfServers = din.readInt();
    servers = new String[numberOfServers];
    for ( int i = 0; i < numberOfServers; ++i ) {
      len = din.readInt();
      array = new byte[len];
      din.readFully( array );
      servers[i] = new String( array );
    }

    din.close();
    bin.close();
  }

  @Override
  public byte getType() {
    return type;
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    byte[] array = filename.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    dout.writeInt( sequence );

    dout.writeInt( servers.length );
    for ( String server : servers ) {
      array = server.getBytes();
      dout.writeInt( array.length );
      dout.write( array );
    }

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }

  public String getFilename() {
    return filename;
  }

  public int getSequence() {
    return sequence;
  }

  public String[] getServers() {
    return servers;
  }
}
