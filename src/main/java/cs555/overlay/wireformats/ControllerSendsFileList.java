package cs555.overlay.wireformats;

import java.io.*;

public class ControllerSendsFileList implements Event {

  private final byte type;
  private String[] list;

  public ControllerSendsFileList(String[] list) {
    this.type = Protocol.CONTROLLER_SENDS_FILE_LIST;
    this.list = list;
  }

  public ControllerSendsFileList(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int listLength = din.readInt();
    if ( listLength != 0 ) {
      list = new String[listLength];
      for ( int i = 0; i < listLength; ++i ) {
        int len = din.readInt();
        byte[] array = new byte[len];
        din.readFully( array );
        list[i] = new String( array );
      }
    }

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream( bout );

    dout.write( type );

    if ( list == null ) {
      dout.writeInt( 0 );
    } else {
      dout.writeInt( list.length );
      for ( String s : list ) {
        byte[] array = s.getBytes();
        dout.writeInt( array.length );
        dout.write( array );
      }
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

  /**
   * Getter for file list.
   *
   * @return String[] of filenames
   */
  public String[] getList() {return list;}
}