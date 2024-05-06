package cs555.overlay.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class used to keep track of the files the ChunkServer is supposed to be
 * storing. All store, repair, and delete operations (those that actually affect
 * what is written on disk), will first modify this map.
 *
 * @author hayne
 */
public class FileMap {
  public record MetaRecord(FileMetadata md, ReentrantLock lock) {}

  private final ConcurrentHashMap<String,MetaRecord> files;

  public FileMap() {
    this.files = new ConcurrentHashMap<>();
  }

  /**
   * Getter for the map.
   *
   * @return files
   */
  public ConcurrentHashMap<String,MetaRecord> getMap() {
    return files;
  }

  /**
   * Gets a metadata record for a particular filename. Once the function
   * returns, the returned metadata record will be locked until the caller
   * unlocks the lock associated with the metadata.
   *
   * @param filename the filename of the metadata record to get
   * @return a MetaRecord containing a FileMetadata, and ReentrantLock
   * controlling access to the metadata
   */
  public MetaRecord get(String filename) {
    return files.compute(filename, (key, value) -> {
      if (value == null) {
        MetaRecord newValue = new MetaRecord(
            new FileMetadata(filename, 0, System.currentTimeMillis()),
            new ReentrantLock(true));
        newValue.lock.lock();
        return newValue;
      }
      value.lock.lock();
      return value;
    });
  }

  /**
   * Gets a metadata record from the map if one exists.
   *
   * @param filename the filename of the metadata record to get
   * @return a MetaRecord containing a FileMetadata, and ReentrantLock
   * controlling access to the Metadata
   */
  public MetaRecord getIfExists(String filename) {
    return files.computeIfPresent(filename, (kye, value) -> {
      value.lock.lock();
      return value;
    });
  }
}