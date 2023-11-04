package cs555.overlay.util;

public class FileMetadata {

  private final String filename; // will include _chunk and/or _shard
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

  @Override
  public boolean equals(Object o) {
    if ( o == this ) {
      return true;
    }
    if ( !(o instanceof FileMetadata fileData) ) {
      return false;
    }

    // May need to modify this to ignore the version number,as a
    // different version number doesn't imply different content.
    return this.filename.equals( fileData.filename ) &&
           this.version == fileData.version;
  }
}