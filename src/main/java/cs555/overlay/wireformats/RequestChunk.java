package cs555.overlay.wireformats;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.FileUtilities;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RequestChunk implements Event {

  private final byte type;
  private final String filename;
  private final String[] servers;
  private final byte[][] pieces; // slices or fragments

  private final String clientAddress;
  private final List<String> route;
  private final List<String> corruptServers;

  public RequestChunk(String filename, String[] servers, String clientAddress) {
    this.type = Protocol.REQUEST_CHUNK;
    this.filename = filename;
    this.servers = servers;
    this.clientAddress = clientAddress;
    this.pieces = new byte[ApplicationProperties.storageType.equals("erasure") ?
                               Constants.TOTAL_SHARDS : Constants.SLICES][];
    this.route = new ArrayList<>(List.of(servers));
    route.removeIf(Objects::isNull);
    Collections.shuffle(route);
    this.corruptServers = new ArrayList<>();
  }

  public RequestChunk(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    this.type = din.readByte();

    int filenameLen = din.readInt();
    this.filename = new String(din.readNBytes(filenameLen));

    int numServers = din.readInt();
    this.servers = new String[numServers];
    for (int i = 0; i < numServers; ++i) {
      int len = din.readInt();
      servers[i] = new String(din.readNBytes(len));
    }

    int addressLen = din.readInt();
    this.clientAddress = new String(din.readNBytes(addressLen));

    int numPieces = din.readInt();
    this.pieces = new byte[numPieces][];
    for (int i = 0; i < numPieces; ++i) {
      int len = din.readInt();
      if (len != 0) {
        pieces[i] = din.readNBytes(len);
      }
    }

    int routeSize = din.readInt();
    this.route = new ArrayList<>(routeSize);
    for (int i = 0; i < routeSize; ++i) {
      int len = din.readInt();
      route.add(new String(din.readNBytes(len)));
    }

    int corruptSize = din.readInt();
    this.corruptServers = new ArrayList<>(corruptSize);
    for (int i = 0; i < corruptSize; ++i) {
      int len = din.readInt();
      corruptServers.add(new String(din.readNBytes(len)));
    }

    din.close();
    bin.close();
  }

  public String getFilename() {
    return filename;
  }

  public String[] getServers() {
    return servers;
  }

  public String getFilenameAtServer() {
    if (ApplicationProperties.storageType.equals("erasure")) {
      int index = ArrayUtilities.contains(servers, route.get(0));
      return filename + "_shard" + index;
    }
    return filename;
  }

  public boolean readyToServe() {
    int nulls = ArrayUtilities.countNulls(pieces);
    if (ApplicationProperties.storageType.equals("erasure")) {
      return nulls <= Constants.PARITY_SHARDS;
    }
    return nulls == 0;
  }

  public String getAddress() {
    if (!route.isEmpty()) {
      return route.get(0);
    }
    return null;
  }

  public String getNextAddress() {
    if (!route.isEmpty()) {
      route.remove(0);
    }
    if (!route.isEmpty()) {
      return route.get(0);
    }
    return null;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public List<String> getCorruptServers() {
    return corruptServers;
  }

  public void addCorruptServer(String server) {
    corruptServers.add(server);
  }

  public byte[][] getPieces() {
    return pieces;
  }

  /**
   * Uses the pieces array to reconstruct the content of the chunk. Assumes a
   * call to readyToServe has already returned true.
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

    byte[] filenameBytes = filename.getBytes();
    dout.writeInt(filenameBytes.length);
    dout.write(filenameBytes);

    dout.writeInt(servers.length);
    for (String server : servers) {
      byte[] serverBytes = server.getBytes();
      dout.writeInt(serverBytes.length);
      dout.write(serverBytes);
    }

    byte[] addressBytes = clientAddress.getBytes();
    dout.writeInt(addressBytes.length);
    dout.write(addressBytes);

    dout.writeInt(pieces.length);
    for (byte[] piece : pieces) {
      if (piece == null) {
        dout.writeInt(0);
        continue;
      }
      dout.writeInt(piece.length);
      dout.write(piece);
    }

    dout.writeInt(route.size());
    for (String server : route) {
      byte[] serverBytes = server.getBytes();
      dout.writeInt(serverBytes.length);
      dout.write(serverBytes);
    }

    dout.writeInt(corruptServers.size());
    for (String server : corruptServers) {
      byte[] serverBytes = server.getBytes();
      dout.writeInt(serverBytes.length);
      dout.write(serverBytes);
    }

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }
}
