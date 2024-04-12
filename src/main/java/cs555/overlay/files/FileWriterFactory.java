package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.util.Logger;

/**
 * Creates FileWriter objects based on the name of the file being written. This
 * way, the ChunkServer can deal with a request to write any file without
 * knowing whether it is a chunk or a shard.
 *
 * @author hayne
 */
public class FileWriterFactory {
  private static final Logger logger = Logger.getInstance();
  private static final FileWriterFactory fileWriterFactory =
      new FileWriterFactory(); // singleton factory

  private FileWriterFactory() {
  }

  public static FileWriterFactory getInstance() {
    return fileWriterFactory;
  }

  /**
   * Creates the right type of FileWriter based on the name of the file to be
   * written.
   *
   * @param metadata of file to be written
   * @return fresh FileWriter object, or null if filename doesn't match a file
   * type
   */
  public FileWriter createFileWriter(FileMetadata metadata) {
    if (FilenameUtilities.checkChunkFilename(metadata.getFilename())) {
      return new ChunkWriter(metadata);
    } else if (FilenameUtilities.checkShardFilename(metadata.getFilename())) {
      return new ShardWriter(metadata);
    } else {
      logger.error("FileWriter couldn't be created. " + metadata.getFilename());
      return null;
    }
  }
}