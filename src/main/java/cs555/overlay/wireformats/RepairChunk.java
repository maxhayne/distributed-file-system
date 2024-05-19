package cs555.overlay.wireformats;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.FileUtilities;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;

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
  private int[] piecesToRepair; // slice indices needing repair
  private final String[] servers; // array of host:port addresses to servers
  private byte[][] pieces;
  private final ArrayList<String> repairRoute; // route the message should take

  public RepairChunk(String filename, String destination, String[] servers) {
    this.type = Protocol.REPAIR_CHUNK;
    this.filename = filename;
    this.destination = destination;
    this.piecesToRepair = new int[0];
    this.servers = servers;
    this.pieces = new byte[ApplicationProperties.storageType.equals("erasure") ?
                               Constants.TOTAL_SHARDS : Constants.SLICES][];

    // Generate route for message
    this.repairRoute = new ArrayList<>();
    for (String server : servers) {
      if (server != null && !java.util.Objects.equals(server, destination)) {
        repairRoute.add(server);
      }
    }
    Collections.shuffle(repairRoute); // balance the load
  }

  public void setPiecesToRepair(int[] piecesToRepair) {
    this.piecesToRepair =
        Objects.requireNonNullElseGet(piecesToRepair, () -> new int[0]);
  }

  public int[] getPiecesToRepair() {
    return piecesToRepair;
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
    piecesToRepair = new int[numberOfSlices];
    for (int i = 0; i < numberOfSlices; ++i) {
      piecesToRepair[i] = din.readInt();
    }

    int numberOfServers = din.readInt();
    servers = new String[numberOfServers];
    for (int i = 0; i < numberOfServers; ++i) {
      len = din.readInt();
      array = new byte[len];
      din.readFully(array);
      servers[i] = new String(array);
    }

    int numberOfPieces = din.readInt();
    pieces = new byte[numberOfPieces][];
    for (int i = 0; i < numberOfPieces; ++i) {
      len = din.readInt();
      if (len == 0) {
        pieces[i] = null;
        continue;
      }
      array = new byte[len];
      din.readFully(array);
      pieces[i] = array;
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
   * Returns the name of the file that needs to be repaired for the particular
   * server. Only changes at the specific server for erasure coding.
   *
   * @return filename of file to be retrieved from current server
   */
  public String getFilenameAtServer() {
    if (ApplicationProperties.storageType.equals("erasure")) {
      int index = ArrayUtilities.contains(servers, repairRoute.get(0));
      return filename + "_shard" + index;
    }
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
   * Moves the address of the destination to the head of the repairRoute so the
   * filename is properly formatted.
   */
  public void prepareForDestination() {
    repairRoute.remove(destination);
    repairRoute.add(0, destination);
  }

  public void setPieces(byte[][] pieces) {
    this.pieces = pieces;
  }

  public byte[][] getPieces() {
    return pieces;
  }

  public boolean readyToRepair() {
    if (ApplicationProperties.storageType.equals("erasure")) {
      return ArrayUtilities.countNulls(pieces) <= Constants.PARITY_SHARDS;
    }
    for (int index : piecesToRepair) {
      if (pieces[index] == null) {
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
  public String getNextAddress() {
    if (!repairRoute.isEmpty()) {
      repairRoute.remove(0);
    }
    if (readyToRepair() || repairRoute.isEmpty()) {
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
   * Uses the pieces array to reconstruct the content of the chunk. Assumes a
   * call to readyToRepair() has already returned true.
   *
   * @return byte[] of chunk's content
   */
  public byte[] getChunk() {
    if (ApplicationProperties.storageType.equals("erasure")) {
      byte[][] decoded = FileUtilities.decodeMissingShards(pieces);
      if (decoded != null) {
        return FileUtilities.getContentFromShards(decoded);
      }
    } else {
      byte[] array = ArrayUtilities.combineByteArrays(pieces);
      array = FileUtilities.removeHashesFromChunk(array);
      return FileUtilities.getDataFromChunk(array);
    }
    return null;
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

    dout.writeInt(piecesToRepair.length);
    for (int sliceIndex : piecesToRepair) {
      dout.writeInt(sliceIndex);
    }

    dout.writeInt(servers.length);
    for (String server : servers) {
      array = server.getBytes();
      dout.writeInt(array.length);
      dout.write(array);
    }

    dout.writeInt(pieces.length);
    for (byte[] piece : pieces) {
      if (piece == null) {
        dout.writeInt(0);
        continue;
      }
      dout.writeInt(piece.length);
      dout.write(piece);
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