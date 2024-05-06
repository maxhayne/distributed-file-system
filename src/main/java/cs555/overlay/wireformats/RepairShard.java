package cs555.overlay.wireformats;

import cs555.overlay.config.Constants;
import cs555.overlay.util.ArrayUtilities;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Dispatched by the Controller to be relayed between ChunkServers with
 * fragments of the chunk in need of repair.
 *
 * @author hayne
 */
public class RepairShard implements Event {
  private final byte type;
  private final String filename; // full filename of shard that needs replacing
  private final String[] servers; // array of host:port addresses to servers
  private final byte[][] fragments; // array of fragment arrays
  private final int destination; // position of destination in server list

  private final ArrayList<String> repairRoute; // route the message should take

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
    this.filename = filename.split("_shard")[0];
    this.servers = servers;
    this.destination = ArrayUtilities.contains(servers, destination);
    this.fragments = new byte[Constants.TOTAL_SHARDS][];

    // Generate route for message
    this.repairRoute = new ArrayList<>();
    for (String server : servers) {
      if (server != null && !java.util.Objects.equals(server, destination)) {
        repairRoute.add(server);
      }
    }
    Collections.shuffle(repairRoute); // balance the load

  }

  public RepairShard(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully(array);
    filename = new String(array);

    destination = din.readInt();

    int numberOfServers = din.readInt();
    servers = new String[numberOfServers];
    for (int i = 0; i < numberOfServers; ++i) {
      len = din.readInt();
      if (len == 0) {
        servers[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully(array);
      servers[i] = new String(array);
    }

    int numberOfFragments = din.readInt();
    fragments = new byte[numberOfFragments][];
    for (int i = 0; i < numberOfFragments; ++i) {
      len = din.readInt();
      if (len == 0) {
        fragments[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully(array);
      fragments[i] = array;
    }

    int remainingServers = din.readInt();
    repairRoute = new ArrayList<>(remainingServers);
    for (int i = 0; i < remainingServers; ++i) {
      len = din.readInt();
      array = new byte[len];
      din.readFully(array);
      repairRoute.add(new String(array));
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
   * Returns the address of the current server on the route.
   *
   * @return host:port at the head of repairRoute, or address of destination
   */
  public String getAddress() {
    if (!repairRoute.isEmpty()) {
      return repairRoute.get(0);
    }
    return servers[destination];
  }

  /**
   * Gets the filename of the fragment for the current address.
   *
   * @return string filename of fragment
   */
  public String getFilename() {
    String base = filename + "_shard";
    if (fragmentsCollected() < Constants.DATA_SHARDS &&
        !repairRoute.isEmpty()) {
      return base + ArrayUtilities.contains(servers, repairRoute.get(0));
    }
    return base + destination;
  }

  /**
   * Returns the address of the next server the message should be forwarded to.
   *
   * @return host:port address of server
   */
  public String nextServer() {
    if (!repairRoute.isEmpty()) {
      repairRoute.remove(0);
    }
    if (fragmentsCollected() >= Constants.DATA_SHARDS ||
        repairRoute.isEmpty()) {
      return servers[destination];
    }
    return repairRoute.get(0);
  }

  /**
   * Returns the total number of fragments collected so far.
   *
   * @return total number of fragments collected
   */
  public int fragmentsCollected() {
    int count = 0;
    for (byte[] fragment : fragments) {
      if (fragment != null) {
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
    DataOutputStream dout = new DataOutputStream(bout);

    dout.write(type);

    byte[] array = filename.getBytes();
    dout.writeInt(array.length);
    dout.write(array);

    dout.writeInt(destination);

    dout.writeInt(servers.length);
    for (String server : servers) {
      if (server == null) {
        dout.writeInt(0);
        continue;
      }
      array = server.getBytes();
      dout.writeInt(array.length);
      dout.write(array);
    }

    dout.writeInt(fragments.length);
    for (byte[] fragment : fragments) {
      if (fragment == null) {
        dout.writeInt(0);
        continue;
      }
      dout.writeInt(fragment.length);
      dout.write(fragment);
    }

    dout.writeInt(repairRoute.size());
    for (String address : repairRoute) {
      array = address.getBytes();
      dout.writeInt(array.length);
      dout.write(array);
    }

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }
}