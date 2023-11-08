package cs555.overlay.wireformats;

import cs555.overlay.config.Constants;

import java.io.*;

public class RepairShard implements Event {

  private final byte type;
  private final String filename; // full filename of shard that needs replacing
  private int destination; // position of destination in server list
  private int position; // current position in server list
  private final String[] servers; // array of host:port addresses to servers
  private final byte[][] fragments; // array of fragment arrays

  /**
   * Default constructor. Designed to simplify call for Controller.
   *
   * @param filename of fragment that needs replacing -- should include
   * "_chunk#_shard#"
   * @param destination host:port address of server that needs repair
   * @param servers array of length 'Constants.TOTAL_SHARDS' that contains
   * "host:port" addresses.
   */
  public RepairShard(String filename, String destination, String[] servers) {
    this.type = Protocol.REPAIR_SHARD;
    this.filename = filename.split( "_shard" )[0];
    this.servers = servers;
    for ( int i = 0; i < servers.length; ++i ) {
      if ( java.util.Objects.equals( servers[i], destination ) ) {
        this.destination = i;
        this.position = i;
        break;
      }
    }
    this.nextPosition(); // move position to first valid server
    this.fragments = new byte[Constants.TOTAL_SHARDS][];
  }

  public RepairShard(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
    DataInputStream din = new DataInputStream( bin );

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully( array );
    filename = new String( array );

    destination = din.readInt();

    position = din.readInt();

    int numberOfServers = din.readInt();
    servers = new String[numberOfServers];
    for ( int i = 0; i < numberOfServers; ++i ) {
      len = din.readInt();
      if ( len == 0 ) {
        servers[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully( array );
      servers[i] = new String( array );
    }

    int numberOfFragments = din.readInt();
    fragments = new byte[numberOfFragments][];
    for ( int i = 0; i < numberOfFragments; ++i ) {
      len = din.readInt();
      if ( len == 0 ) {
        fragments[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully( array );
      fragments[i] = array;
    }

    din.close();
    bin.close();
  }

  /**
   * Gets the address host:port of the destination server.
   *
   * @return host:port string
   */
  public String getDestination() {
    return servers[destination];
  }

  /**
   * Returns the host:port string stored at 'servers[position]'.
   *
   * @return host:port stored at server[position]
   */
  public String getAddress() {
    return servers[position];
  }

  /**
   * Gets the filename of the fragment at the current position.
   *
   * @return string filename of fragment
   */
  public String getFilename() {
    return filename+"_shard"+position;
  }

  /**
   * The member 'position' only gets changed when 'nextPosition()' is called,
   * but if we've collected Constants.DATA_SHARDS fragments (6) already, we can
   * go straight to the destination, so we need a way to move the 'position' to
   * match 'destination', so the filename is correct when called at the
   * destination server.
   */
  public void setPositionToDestination() {
    position = destination;
  }

  /**
   * Moves the 'position' integer to the position of the next non-null entry in
   * the 'servers' array. If the next non-null entry in the 'servers' array is
   * the destination, returns false.
   *
   * @return true if the 'position' integer was moved to the index of another
   * non-null server in the 'servers' array, false if there wasn't another
   * non-null server in the array. False implies the message should be forwarded
   * directly to the 'destination' server.
   */
  public boolean nextPosition() {
    int nextPosition = position+1;
    for ( int i = 1; i < Constants.TOTAL_SHARDS; ++i ) {
      nextPosition = nextPosition%Constants.TOTAL_SHARDS;
      if ( nextPosition == destination ) {
        return false;
      } else if ( servers[nextPosition] != null ) {
        break;
      }
      nextPosition++;
    }
    position = nextPosition;
    return true;
  }

  /**
   * Returns the total number of fragments collected so far.
   *
   * @return total number of fragments collected
   */
  public int fragmentsCollected() {
    int count = 0;
    for ( byte[] fragment : fragments ) {
      if ( fragment != null ) {
        count++;
      }
    }
    return count;
  }

  /**
   * Returns the array of fragments that have been collected, however complete.
   *
   * @return byte[][] of fragments
   */
  public byte[][] getFragments() {
    return fragments;
  }

  /**
   * Attach a fragment to the 'fragments' array at a particular index.
   *
   * @param index of fragment to attach
   * @param fragment byte string of valid fragment
   */
  public void attachFragment(int index, byte[] fragment) {
    fragments[index] = fragment;
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

    dout.writeInt( destination );

    dout.writeInt( position );

    dout.writeInt( servers.length );
    for ( String server : servers ) {
      if ( server == null ) {
        dout.writeInt( 0 );
        continue;
      }
      array = server.getBytes();
      dout.writeInt( array.length );
      dout.write( array );
    }

    dout.writeInt( fragments.length );
    for ( byte[] fragment : fragments ) {
      if ( fragment == null ) {
        dout.writeInt( 0 );
        continue;
      }
      dout.writeInt( fragment.length );
      dout.write( fragment );
    }

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }
}