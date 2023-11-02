package cs555.overlay.wireformats;

import java.io.*;

public class ControllerReportsFileSize implements Event {

  private final byte type;
  private final String filename;
  private final int totalChunks;

  public ControllerReportsFileSize(String filename, int totalChunks) {
    this.type = Protocol.CONTROLLER_REPORTS_FILE_SIZE;
    this.filename = filename;
    this.totalChunks = totalChunks;
  }

  public ControllerReportsFileSize(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    totalChunks = din.readInt();

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

    dout.writeInt( totalChunks );

    byte[] returnable = bout.toByteArray();
    dout.close();
    bout.close();
    return returnable;
  }

  @Override
  public byte getType() {
    return type;
  }

  public String getFilename() {
    return filename;
  }

  public int getTotalChunks() {
    return totalChunks;
  }
}