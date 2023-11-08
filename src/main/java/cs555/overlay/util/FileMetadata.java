package cs555.overlay.util;

/**
 * Class to keep track of the metadata associated with files stored on
 * ChunkServers.
 *
 * @author hayne
 */
public class FileMetadata {

  private final String filename; // includes "_chunk#" and "_shard#"
  private int version;
  private long timestamp;

  public FileMetadata(String filename, int version, long timestamp) {
    this.filename = filename;
    this.version = version;
    this.timestamp = timestamp;
  }

  public String getFilename() {
    return filename;
  }

  public int getVersion() {
    return version;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void incrementVersion() {
    version++;
  }

  public void updateTimestamp() {
    timestamp = System.currentTimeMillis();
  }
}