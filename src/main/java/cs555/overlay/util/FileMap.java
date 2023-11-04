package cs555.overlay.util;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class used to keep track of the files the ChunkServer is supposed to be
 * storing. All store, repair, and delete operations (those that actually affect
 * what is written on disk), will first modify this map. Use of these methods
 * assumes the intrinsic lock has already been acquired.
 *
 * @author hayne
 */
public class FileMap {
  private final HashMap<String, FileMetadata> files;

  public FileMap() {
    this.files = new HashMap<>();
  }

  /**
   * Getter for 'files' map.
   *
   * @return files
   */
  public HashMap<String, FileMetadata> getFiles() {
    return files;
  }

  /**
   * Either adds a new file, or updates an existing one by incrementing the
   * version and refreshing the timestamp.
   *
   * @param filename of metadata to be created or updated
   * @return metadata either newly created or updated
   */
  public FileMetadata addOrUpdate(String filename) {
    if ( files.containsKey( filename ) ) {
      files.get( filename ).incrementVersion();
      files.get( filename ).updateTimestamp();
    } else {
      files.put( filename,
          new FileMetadata( filename, 0, System.currentTimeMillis() ) );
    }
    return files.get( filename );
  }

  /**
   * Deletes all files with the basename filename from the 'files' map.
   *
   * @param filename basename to delete from 'files'
   */
  public void deleteFile(String filename) {
    ArrayList<String> remove = new ArrayList<>();
    for ( FileMetadata meta : files.values() ) {
      if ( meta.getFilename().split( "_chunk" )[0].equals( filename ) ) {
        remove.add( meta.getFilename() );
      }
    }
    for ( String name : remove ) {
      files.remove( name );
    }
  }
}
