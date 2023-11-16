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
  private boolean isNew;

  public FileMetadata(String filename, int version, long timestamp) {
    this.filename = filename;
    this.version = version;
    this.timestamp = timestamp;
    this.isNew = true;
  }

  public String getFilename() {return filename;}

  public synchronized int getVersion() {
    return version;
  }

  public synchronized long getTimestamp() {
    return timestamp;
  }

  public synchronized boolean isNew() {return isNew;}

  public synchronized void notNew() {
    isNew = false;
  }

  public synchronized void update() {
    version++;
    timestamp = System.currentTimeMillis();
  }
}