package cs555.overlay.files;

import cs555.overlay.util.FilenameUtilities;

/**
 * Creates FileReader objects based on the name of the file being read. This
 * way, the ChunkServer can deal with a request to read any file without knowing
 * whether it is a chunk or a shard.
 *
 * @author hayne
 */
public class FileReaderFactory {
  private static final FileReaderFactory fileReaderFactory =
      new FileReaderFactory(); // singleton factory

  private FileReaderFactory() {}

  public static FileReaderFactory getInstance() {
    return fileReaderFactory;
  }

  /**
   * Creates the right type of FileReader based on the name of the file to be
   * read.
   *
   * @param filename of file to be read
   * @return fresh FileReader object, or null if filename doesn't match a file
   * type
   */
  public FileReader createFileReader(String filename) {
    if ( FilenameUtilities.checkChunkFilename( filename ) ) {
      return new ChunkReader( filename );
    } else if ( FilenameUtilities.checkShardFilename( filename ) ) {
      return new ShardReader( filename );
    } else {
      System.err.println(
          "createFileReader: FileReader couldn't be created. "+filename );
      return null;
    }
  }
}
