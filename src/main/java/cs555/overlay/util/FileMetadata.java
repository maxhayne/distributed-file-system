package cs555.overlay.util;

/**
 * Used to keep track of metadata associated with files stored on ChunkServers.
 *
 * @author hayne
 */
public class FileMetadata {
  private final String filename; // includes "_chunk#" and "_shard#"
  private int version;
  private long timestamp;
  private boolean written; // written to disk?

  /**
   * Constructor.
   *
   * @param filename filename of chunk/shard
   * @param version version of file
   * @param timestamp timestamp of most recent modification
   */
  public FileMetadata(String filename, int version, long timestamp) {
    this.filename = filename;
    this.version = version;
    this.timestamp = timestamp;
    this.written = false;
  }

  /**
   * Filename getter.
   *
   * @return filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Version getter.
   *
   * @return version
   */
  public synchronized int getVersion() {
    return version;
  }

  /**
   * Timestamp getter.
   *
   * @return timestamp
   */
  public synchronized long getTimestamp() {
    return timestamp;
  }

  /**
   * Sets written to false.
   */
  public synchronized void written() {
    written = true;
  }

  /**
   * Updates the version and timestamp if written has already been set to true.
   */
  public synchronized void updateIfWritten() {
    if (!written) {
      written();
    } else {
      update();
    }
  }

  /**
   * Updates the version and timestamp.
   */
  public synchronized void update() {
    timestamp = System.currentTimeMillis();
    version++;
  }
}