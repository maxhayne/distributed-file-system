package cs555.overlay.wireformats;

import java.io.*;

/**
 * Sent by the Controller to the Client containing information about where all
 * the chunks of a file are stored on the DFS.
 *
 * @author hayne
 */
public class ControllerSendsStorageList implements Event {
  private final byte type;
  private final String filename;
  private final String[][] servers;

  public ControllerSendsStorageList(String filename, String[][] servers) {
    this.type = Protocol.CONTROLLER_SENDS_STORAGE_LIST;
    this.filename = filename;
    this.servers = servers;
  }

  public ControllerSendsStorageList(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully(array);
    filename = new String(array);

    int chunkCount = din.readInt();
    if (chunkCount != 0) {
      servers = new String[chunkCount][];
      for (int i = 0; i < chunkCount; ++i) {
        int serverCount = din.readInt();
        servers[i] = new String[serverCount];
        for (int j = 0; j < serverCount; ++j) {
          len = din.readInt();
          if (len == 0) {
            servers[i][j] = null;
          } else {
            array = new byte[len];
            din.readFully(array);
            servers[i][j] = new String(array);
          }
        }
      }
    } else {
      servers = null;
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

    // Write list of servers
    int chunkCount = servers != null ? servers.length : 0;
    dout.writeInt(chunkCount);
    for (int i = 0; i < chunkCount; ++i) {
      dout.writeInt(servers[i].length);
      for (int j = 0; j < servers[i].length; ++j) {
        if (servers[i][j] == null) {
          dout.writeInt(0);
        } else {
          array = servers[i][j].getBytes();
          dout.writeInt(array.length);
          dout.write(array);
        }
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

  public String getFilename() {
    return filename;
  }

  public String[][] getServers() {
    return servers;
  }
}
