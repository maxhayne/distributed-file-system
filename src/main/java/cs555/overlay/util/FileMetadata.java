package cs555.overlay.util;

/**
 * Class to keep track of the metadata associated with files stored on
 * ChunkServers. Has a secondary use as a lock for accessing files at the
 * ChunkServer.
 *
 * @author hayne
 */
public class FileMetadata {
  private final String filename; // includes "_chunk#" and "_shard#"
  private int version;
  private long timestamp;
  private boolean isNew;

  /**
   * Constructor. Sets isNew to true automatically.
   *
   * @param filename filename of chunk/shard
   * @param version version of file
   * @param timestamp timestamp of most recent modification
   */
  public FileMetadata(String filename, int version, long timestamp) {
    this.filename = filename;
    this.version = version;
    this.timestamp = timestamp;
    this.isNew = true;
  }

  /**
   * Filename getter.
   *
   * @return filename
   */
  public String getFilename() {return filename;}

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
   * Sets isNew to false.
   */
  public synchronized void notNew() {
    isNew = false;
  }

  /**
   * Updates the version and timestamp if isNew is false. Otherwise, it sets
   * isNew to true.
   */
  public synchronized void updateIfNotNew() {
    if ( isNew ) {
      notNew();
    } else {
      update();
    }
  }

  /**
   * Updates the version and timestamp.
   */
  public synchronized void update() {
    version++;
    timestamp = System.currentTimeMillis();
  }
}