package cs555.overlay.wireformats;

import java.io.*;

/**
 * Message used by a ChunkServer to send a file's data to a Client.
 *
 * @author hayne
 */
public class ChunkServerServesFile implements Event {
  private final byte type;
  private final String filename;
  private final byte[] content;

  public ChunkServerServesFile(String filename, byte[] content) {
    this.type = Protocol.CHUNK_SERVER_SERVES_FILE;
    this.filename = filename;
    this.content = content;
  }

  public ChunkServerServesFile(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    int len = din.readInt();
    byte[] array = new byte[len];
    din.readFully(array);
    filename = new String(array);

    len = din.readInt();
    content = new byte[len];
    din.readFully(content);

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
    dout.write(content);

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
   * Getter for filename.
   *
   * @return filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Getter for content.
   *
   * @return byte[] content of served file
   */
  public byte[] getContent() {
    return content;
  }
}