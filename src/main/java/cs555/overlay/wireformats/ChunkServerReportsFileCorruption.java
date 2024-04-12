package cs555.overlay.wireformats;

import java.io.*;

/**
 * Message sent to Controller by ChunkServer whenever a corrupt chunk or shard
 * is detected.
 *
 * @author hayne
 */
public class ChunkServerReportsFileCorruption implements Event {

  private final byte type;
  private final int identifier;
  private final String filename;
  private final int[] slices;

  public ChunkServerReportsFileCorruption(int identifier, String filename,
      int[] slices) {
    this.type = Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION;
    this.identifier = identifier;
    this.filename = filename;
    this.slices = slices;
  }

  public ChunkServerReportsFileCorruption(byte[] marshalledBytes)
      throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    identifier = din.readInt();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully(array);
    filename = new String(array);

    int numSlices = din.readInt();
    if (numSlices > 0) {
      slices = new int[numSlices];
      for (int i = 0; i < numSlices; ++i) {
        slices[i] = din.readInt();
      }
    } else {
      slices = null;
    }

    din.close();
    bin.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);

    dout.write(type);

    dout.writeInt(identifier);

    byte[] array = filename.getBytes();
    dout.writeInt(array.length);
    dout.write(array);

    if (slices == null) {
      dout.writeInt(0);
    } else {
      dout.writeInt(slices.length);
      for (int slice : slices) {
        dout.writeInt(slice);
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
   * Getter for filename.
   *
   * @return filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Getter for identifier.
   *
   * @return identifier
   */
  public int getIdentifier() {
    return identifier;
  }

  /**
   * Getter for slices that are corrupt.
   *
   * @return int[] of corrupt slice indices
   */
  public int[] getSlices() {
    return slices;
  }
}