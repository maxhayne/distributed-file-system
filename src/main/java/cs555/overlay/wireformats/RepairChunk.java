package cs555.overlay.wireformats;

import java.io.*;

public class RepairChunk implements Event {

  private final byte type;
  private final String filename; // including _chunk#
  private final String destination; // host:port of server that needs repair
  private final int[] slicesToRepair;
      // array of slice numbers that need repairing
  private final String[] servers; // array of host:port addresses to servers
  private final byte[][] replacementSlices; // array for chunk slices that have
  // been retrieved so far
  private int position; // current position in server list

  public RepairChunk(String filename, String destination, int[] slicesToRepair,
      String[] servers) {
    this.type = Protocol.REPAIR_CHUNK;
    this.filename = filename;
    this.destination = destination;
    this.slicesToRepair = slicesToRepair;
    this.servers = servers;
    this.replacementSlices = new byte[slicesToRepair.length][];
    this.position = 0;
  }

  public RepairChunk(byte[] marshalledBytes) throws IOException {
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
    destination = new String( array );

    int numberOfSlices = din.readInt();
    slicesToRepair = new int[numberOfSlices];
    for ( int i = 0; i < numberOfSlices; ++i ) {
      slicesToRepair[i] = din.readInt();
    }

    int numberOfServers = din.readInt();
    servers = new String[numberOfServers];
    for ( int i = 0; i < numberOfServers; ++i ) {
      len = din.readInt();
      array = new byte[len];
      din.readFully( array );
      servers[i] = new String( array );
    }

    int numberOfReplacementSlices = din.readInt();
    replacementSlices = new byte[numberOfReplacementSlices][];
    for ( int i = 0; i < numberOfReplacementSlices; ++i ) {
      len = din.readInt();
      if ( len == 0 ) {
        replacementSlices[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully( array );
      replacementSlices[i] = array;
    }

    position = din.readInt();

    din.close();
    bin.close();
  }

  /**
   * Returns filename of chunk to repair.
   *
   * @return chunk filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Returns host:port address of server that needs repair.
   *
   * @return destination host:port
   */
  public String getDestination() {
    return destination;
  }

  /**
   * Sets a byte string to the position of a particular slice in the
   * 'replacementSlices' array.
   *
   * @param sliceIndex the number of the slice that will be sliceBytes
   * @param sliceBytes the replacement slice byte string
   */
  public void addSlice(int sliceIndex, byte[] sliceBytes) {
    for ( int i = 0; i < slicesToRepair.length; ++i ) {
      if ( slicesToRepair[i] == sliceIndex ) {
        replacementSlices[i] = sliceBytes;
        return;
      }
    }
  }

  /**
   * Checks if replacements to all slices have been added.
   *
   * @return true if all slots in "replacementSlices' are non-null, false
   * otherwise
   */
  public boolean allSlicesRetrieved() {
    for ( byte[] sliceBytes : replacementSlices ) {
      if ( sliceBytes == null ) {
        return false;
      }
    }
    return true;
  }

  /**
   * Moves the 'position' integer to the next server in the 'servers' array and
   * returns true. If the end of the array has already been reached, returns
   * false.
   *
   * @return true if there's another server to visit, false if there isn't.
   * False implies that the message should be forwarded directly to the
   * 'destination' server.
   */
  public boolean nextPosition() {
    if ( position < servers.length-1 ) {
      position++;
      return true;
    }
    return false;
  }

  /**
   * Returns the host:port address specified at the index 'position' in the
   * 'servers' array.
   *
   * @return host:port address specified at servers[position]
   */
  public String getAddress() {
    return servers[position];
  }

  /**
   * Returns an int[] containing the indices of the slices that still need
   * repairing -- that is, slice indices in 'slicesToRepair' that are still set
   * to null in 'replacementSlices'.
   *
   * @return int[] containing the indices of slices that need repairing, null if
   * there are no slices left to repair
   */
  public int[] getSlicesToRepair() {
    int count = 0;
    for ( byte[] replacementSlice : replacementSlices ) {
      if ( replacementSlice == null ) {
        count++;
      }
    }
    int[] slicesNeedingRepair = new int[count];
    int index = 0;
    for ( int i = 0; i < replacementSlices.length; ++i ) {
      if ( replacementSlices[i] == null ) {
        slicesNeedingRepair[index] = slicesToRepair[i];
        index++;
      }
    }
    return slicesNeedingRepair;
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

    array = destination.getBytes();
    dout.writeInt( array.length );
    dout.write( array );

    dout.writeInt( slicesToRepair.length );
    for ( int sliceIndex : slicesToRepair ) {
      dout.writeInt( sliceIndex );
    }

    dout.writeInt( servers.length );
    for ( String server : servers ) {
      array = server.getBytes();
      dout.writeInt( array.length );
      dout.write( array );
    }

    dout.writeInt( replacementSlices.length );
    for ( byte[] replacementSlice : replacementSlices ) {
      if ( replacementSlice == null ) {
        dout.writeInt( 0 );
        continue;
      }
      dout.writeInt( replacementSlice.length );
      dout.write( replacementSlice );
    }

    dout.writeInt( position );

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }
}
