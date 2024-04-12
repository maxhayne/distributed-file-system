package cs555.overlay.wireformats;

import cs555.overlay.util.FileMetadata;

import java.io.*;
import java.util.ArrayList;

/**
 * For the ChunkServer to send heartbeat information to the Controller.
 *
 * @author hayne
 */
public class ChunkServerSendsHeartbeat implements Event {

  private final byte type;
  private final int identifier;
  private final int beatType; // 1 is major, 0 is minor
  private final int totalChunks;
  private final long freeSpace;
  private final ArrayList<FileMetadata> files;

  public ChunkServerSendsHeartbeat(int identifier, int beatType,
      int totalChunks, long freeSpace, ArrayList<FileMetadata> files) {
    this.type = Protocol.CHUNK_SERVER_SENDS_HEARTBEAT;
    this.identifier = identifier;
    this.beatType = beatType;
    this.totalChunks = totalChunks;
    this.freeSpace = freeSpace;
    this.files = files;
  }

  public ChunkServerSendsHeartbeat(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(marshalledBytes);
    DataInputStream din = new DataInputStream(bin);

    type = din.readByte();

    identifier = din.readInt();

    beatType = din.readInt();

    totalChunks = din.readInt();

    freeSpace = din.readLong();

    int totalFiles = din.readInt();
    files = new ArrayList<>();
    for (int i = 0; i < totalFiles; ++i) {
      int len = din.readInt();
      byte[] array = new byte[len];
      din.readFully(array);
      String filename = new String(array);

      int version = din.readInt();

      long timestamp = din.readLong();

      files.add(new FileMetadata(filename, version, timestamp));
    }

    bin.close();
    din.close();
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream((bout));

    dout.writeByte(type);

    dout.writeInt(identifier);

    dout.writeInt(beatType);

    dout.writeInt(totalChunks);

    dout.writeLong(freeSpace);

    dout.writeInt(files.size());
    for (FileMetadata meta : files) {
      byte[] array = meta.getFilename().getBytes();
      dout.writeInt(array.length);
      dout.write(array);

      dout.writeInt(meta.getVersion());

      dout.writeLong(meta.getTimestamp());
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

  public int getIdentifier() {
    return identifier;
  }

  public int getBeatType() {
    return beatType;
  }

  public int getTotalChunks() {
    return totalChunks;
  }

  public long getFreeSpace() {
    return freeSpace;
  }

  public ArrayList<FileMetadata> getFiles() {
    return files;
  }
}