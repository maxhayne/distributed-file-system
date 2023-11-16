package cs555.overlay.util;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class used to keep track of the files the ChunkServer is supposed to be
 * storing. All store, repair, and delete operations (those that actually affect
 * what is written on disk), will first modify this map.
 *
 * @author hayne
 */
public class FileMap {
  private final ConcurrentHashMap<String, FileMetadata> files;

  public FileMap() {
    this.files = new ConcurrentHashMap<>();
  }

  /**
   * Getter for the map.
   *
   * @return files
   */
  public ConcurrentHashMap<String, FileMetadata> getMap() {
    return files;
  }

  /**
   * Returns a FileMetadata object with a particular filename. If the map
   * doesn't already contain filename as a key, creates a new entry and returns
   * that entry. Otherwise, returns the already existing entry. There is no
   * guarantee that by the time the function returns, the fetched FileMetadata
   * is still a value in the map.
   *
   * @param filename of FileMetadata to be gotten
   * @return FileMetadata object stored in the files map
   */
  public FileMetadata get(String filename) {
    FileMetadata meta = files.get( filename );
    if ( meta == null ) {
      meta = new FileMetadata( filename, 0, System.currentTimeMillis() );
      FileMetadata put = files.putIfAbsent( filename, meta );
      return put == null ? meta : put;
    } else {
      return meta;
    }
  }

  /**
   * Deletes all files with the basename filename from the map.
   *
   * @param filename basename to delete from 'files'
   */
  public ArrayList<String> deleteFile(String filename) {
    ArrayList<String> deletedFiles = new ArrayList<>();
    files.forEach( (name, metadata) -> {
      if ( FilenameUtilities.getBaseFilename( name ).equals( filename ) ) {
        deletedFiles.add( name );
        files.remove( name );
      }
    } );
    return deletedFiles;
  }
}