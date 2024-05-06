package cs555.overlay.wireformats;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Dispatched by the Controller to be relayed between ChunkServers with
 * uncorrupted slices of the chunk in need of a repair.
 *
 * @author hayne
 */
public class RepairChunk implements Event {
  private final byte type;
  private final String filename; // including _chunk#
  private final String destination; // host:port of server that needs repair
  private final int[] slicesToRepair;// slice numbers needing repair
  private final String[] servers; // array of host:port addresses to servers
  private final byte[][] replacementSlices; // slices retrieved so far w/ hashes

  private final ArrayList<String> repairRoute; // route the message should take

  public RepairChunk(String filename, String destination, int[] slicesToRepair,
      String[] servers) {
    this.type = Protocol.REPAIR_CHUNK;
    this.filename = filename;
    this.destination = destination;
    this.slicesToRepair = slicesToRepair;
    this.servers = servers;
    this.replacementSlices = new byte[slicesToRepair.length][];

    // Generate route for message
    this.repairRoute = new ArrayList<>();
    for (String server : servers) {
      if (server != null && !java.util.Objects.equals(server, destination)) {
        repairRoute.add(server);
      }
    }
    Collections.shuffle(repairRoute); // balance the load
  }

  public RepairChunk(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully(array);
    filename = new String(array);

    len = din.readInt();
    array = new byte[len];
    din.readFully(array);
    destination = new String(array);

    int numberOfSlices = din.readInt();
    slicesToRepair = new int[numberOfSlices];
    for (int i = 0; i < numberOfSlices; ++i) {
      slicesToRepair[i] = din.readInt();
    }

    int numberOfServers = din.readInt();
    servers = new String[numberOfServers];
    for (int i = 0; i < numberOfServers; ++i) {
      len = din.readInt();
      array = new byte[len];
      din.readFully(array);
      servers[i] = new String(array);
    }

    int numberOfReplacementSlices = din.readInt();
    replacementSlices = new byte[numberOfReplacementSlices][];
    for (int i = 0; i < numberOfReplacementSlices; ++i) {
      len = din.readInt();
      if (len == 0) {
        replacementSlices[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully(array);
      replacementSlices[i] = array;
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
  public void attachSlice(int sliceIndex, byte[] sliceBytes) {
    for (int i = 0; i < slicesToRepair.length; ++i) {
      if (slicesToRepair[i] == sliceIndex) {
        replacementSlices[i] = sliceBytes;
        return;
      }
    }
  }

  /**
   * Checks if replacements to all slices have been added.
   *
   * @return true if all slots in 'replacementSlices' are non-null, false
   * otherwise
   */
  public boolean allSlicesRetrieved() {
    for (byte[] replacementSlice : replacementSlices) {
      if (replacementSlice == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the address of the next server the message should be forwarded to.
   *
   * @return host:port address of next server
   */
  public String nextServer() {
    if (!repairRoute.isEmpty()) {
      repairRoute.remove(0);
    }
    if (allSlicesRetrieved() || repairRoute.isEmpty()) {
      return destination;
    }
    return repairRoute.get(0);
  }

  /**
   * Returns the address of the current server on the route.
   *
   * @return host:port at head of repairRoute, or address of destination
   */
  public String getAddress() {
    if (!repairRoute.isEmpty()) {
      return repairRoute.get(0);
    }
    return destination;
  }

  /**
   * Returns an int[] containing the indices of the slices that still need
   * repairing -- that is, slice indices in 'slicesToRepair' that are still set
   * to null in 'replacementSlices'.
   *
   * @return int[] containing the indices of slices that need repairing, null if
   * there are no slices left to repair
   */
  public int[] slicesStillNeedingRepair() {
    int count = 0;
    for (byte[] replacementSlice : replacementSlices) {
      if (replacementSlice == null) {
        count++;
      }
    }
    int[] slicesNeedingRepair = new int[count];
    int index = 0;
    for (int i = 0; i < replacementSlices.length; ++i) {
      if (replacementSlices[i] == null) {
        slicesNeedingRepair[index] = slicesToRepair[i];
        index++;
      }
    }
    return slicesNeedingRepair;
  }

  /**
   * Returns an int[] containing the slice indices that correspond to non-null
   * entries in the replacementSlices array.
   *
   * @return int[] of slice indices retrieved thus far, null if no slices have
   * been retrieved thus far
   */
  public int[] getRepairedIndices() {
    int totalSlicesRetrieved = 0;
    for (byte[] replacementSlice : replacementSlices) {
      if (replacementSlice != null) {
        totalSlicesRetrieved++;
      }
    }
    if (totalSlicesRetrieved == 0) {
      return null;
    }
    int[] repairedSlices = new int[totalSlicesRetrieved];
    int index = 0;
    for (int i = 0; i < replacementSlices.length; ++i) {
      if (replacementSlices[i] != null) {
        repairedSlices[index] = slicesToRepair[i];
        index++;
      }
    }
    return repairedSlices;
  }

  /**
   * Get array of non-null slices that have been retrieved so far.
   *
   * @return array of slices that have been retrieved so far
   */
  public byte[][] getReplacedSlices() {
    int totalSlicesRetrieved = 0;
    for (byte[] replacementSlice : replacementSlices) {
      if (replacementSlice != null) {
        totalSlicesRetrieved++;
      }
    }
    if (totalSlicesRetrieved == 0) {
      return null;
    }
    byte[][] slicesRetrieved = new byte[totalSlicesRetrieved][];
    int index = 0;
    for (byte[] replacementSlice : replacementSlices) {
      if (replacementSlice != null) {
        slicesRetrieved[index] = replacementSlice;
        index++;
      }
    }
    return slicesRetrieved;
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

    array = destination.getBytes();
    dout.writeInt(array.length);
    dout.write(array);

    dout.writeInt(slicesToRepair.length);
    for (int sliceIndex : slicesToRepair) {
      dout.writeInt(sliceIndex);
    }

    dout.writeInt(servers.length);
    for (String server : servers) {
      array = server.getBytes();
      dout.writeInt(array.length);
      dout.write(array);
    }

    dout.writeInt(replacementSlices.length);
    for (byte[] replacementSlice : replacementSlices) {
      if (replacementSlice == null) {
        dout.writeInt(0);
        continue;
      }
      dout.writeInt(replacementSlice.length);
      dout.write(replacementSlice);
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