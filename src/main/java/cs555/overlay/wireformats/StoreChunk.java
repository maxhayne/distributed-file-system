package cs555.overlay.wireformats;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.util.ArrayUtilities;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sent by the Controller to a ChunkServer, carrying the content of a chunk to
 * be written to disk. Message will be forwarded to other ChunkServers that must
 * store the same chunk.
 *
 * @author hayne
 */
public class StoreChunk implements Event {

  private final byte type;
  private final String filename;
  private final byte[][] content;
  private String[] servers;
  private final List<String> route;

  /**
   * Constructor.
   *
   * @param filename of file to be stored
   * @param content of file to be stored
   * @param servers to store the file at
   */
  public StoreChunk(String filename, byte[][] content, String[] servers) {
    this.type = Protocol.STORE_CHUNK;
    this.filename = filename;
    this.content = content;
    this.servers = servers;
    this.route = new ArrayList<String>(List.of(servers));
    Collections.shuffle(route);
  }

  public StoreChunk(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully(array);
    filename = new String(array);

    int numArrays = din.readInt();
    content = new byte[numArrays][];
    for (int i = 0; i < numArrays; ++i) {
      len = din.readInt();
      if (len != 0) {
        content[i] = new byte[len];
        din.readFully(content[i]);
      }
    }

    int numServers = din.readInt();
    if (numServers != 0) {
      servers = new String[numServers];
      for (int i = 0; i < numServers; ++i) {
        len = din.readInt();
        array = new byte[len];
        din.readFully(array);
        servers[i] = new String(array);
      }
    }

    int routeLength = din.readInt();
    route = new ArrayList<>(routeLength);
    for (int i = 0; i < routeLength; ++i) {
      int serverLength = din.readInt();
      byte[] serverBytes = din.readNBytes(serverLength);
      route.add(new String(serverBytes));
    }

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);

    dout.write(type);

    byte[] array = filename.getBytes();
    dout.writeInt(array.length);
    dout.write(array);

    dout.writeInt(content.length);
    for (byte[] data : content) {
      if (data == null) {
        dout.writeInt(0);
      } else {
        dout.writeInt(data.length);
        dout.write(data);
      }
    }

    if (servers != null) {
      dout.writeInt(servers.length);
      for (String server : servers) {
        array = server.getBytes();
        dout.writeInt(array.length);
        dout.write(array);
      }
    } else {
      dout.writeInt(0);
    }

    dout.writeInt(route.size());
    for (String server : route) {
      byte[] serverBytes = server.getBytes();
      dout.writeInt(serverBytes.length);
      dout.write(serverBytes);
    }

    byte[] marshalledBytes = bout.toByteArray();
    dout.close();
    bout.close();
    return marshalledBytes;
  }

  @Override
  public byte getType() {
    return type;
  }

  /**
   * Getter for filename to be stored. If the file being stored is a fragment,
   * the filename will change from server to server.
   *
   * @return filename string
   */
  public String getFilenameAtServer() {
    if (ApplicationProperties.storageType.equals("erasure")) {
      int index = ArrayUtilities.contains(servers, route.get(0));
      return filename + "_shard" + index;
    } else {
      return filename;
    }
  }

  /**
   * Return the address of the server at the head of route.
   *
   * @return host:port of current server
   */
  public String getServer() {
    if (!route.isEmpty()) {
      return route.get(0);
    }
    return null;
  }

  /**
   * Returns the address of the next server the message should be forwarded to.
   *
   * @return host:port address of next server
   */
  public String getNextServer() {
    if (!route.isEmpty()) {
      route.remove(0);
    }
    if (route.isEmpty()) {
      return null;
    }
    return route.get(0);
  }

  /**
   * Returns the file's content. If using erasure coding, every server will
   * store a different fragment, but if we're replicating, every server will
   * store the same thing.
   *
   * @return byte[] of file's content
   */
  public byte[] getContent() {
    if (ApplicationProperties.storageType.equals("erasure")) {
      int index = ArrayUtilities.contains(servers, route.get(0));
      byte[] copy = new byte[content[index].length];
      System.arraycopy(content[index], 0, copy, 0, copy.length);
      content[index] = null; // So we don't relay it to the next ChunkServer
      return copy;
    } else {
      return content[0];
    }
  }
}