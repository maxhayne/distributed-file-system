package cs555.overlay.files;

import cs555.overlay.util.FileSynchronizer;

/**
 * Creates FileWriter objects based on the name of the file being written. This
 * way, the ChunkServer can deal with a request to write any file without
 * knowing whether it is a chunk or a shard.
 *
 * @author hayne
 */
public class FileWriterFactory {
  private static final FileWriterFactory fileWriterFactory =
      new FileWriterFactory(); // singleton factory

  private FileWriterFactory() {}

  public static FileWriterFactory getInstance() {
    return fileWriterFactory;
  }

  /**
   * Creates the right type of FileWriter based on the name of the file to be
   * written.
   *
   * @param filename of file to be written
   * @return fresh FileWriter object, or null if filename doesn't match a file
   * type
   */
  public FileWriter createFileWriter(String filename, byte[] content) {
    if ( FileSynchronizer.checkChunkFilename( filename ) ) {
      return new ChunkWriter( filename, content );
    } else if ( FileSynchronizer.checkShardFilename( filename ) ) {
      return new ShardWriter( filename, content );
    } else {
      System.err.println(
          "createFileWriter: FileWriter couldn't be created. "+filename );
      return null;
    }
  }

  public FileWriter createFileWriter(FileReader reader) {
    if ( FileSynchronizer.checkChunkFilename( reader.getFilename() ) ) {
      return new ChunkWriter( reader );
    } else if ( FileSynchronizer.checkShardFilename(
        reader.getFilename() ) ) {
      return new ShardWriter( reader );
    } else {
      System.err.println( "createFileWriter: FileWriter couldn't be created. "+
                          reader.getFilename() );
      return null;
    }
  }

}