package cs555.overlay.files;

import cs555.overlay.util.FileDistributionService;

/**
 * Creates FileReader objects based on the name of the file being read. This
 * way, the ChunkServer can deal with a request to read any file without knowing
 * whether it is a chunk or a shard.
 *
 * @author hayne
 */
public class FileReaderFactory {
  private static final FileReaderFactory fileReaderFactory =
      new FileReaderFactory();

  private FileReaderFactory() {}

  public static FileReaderFactory getInstance() {
    return fileReaderFactory;
  }

  public FileReader createFileReader(String filename) {
    if ( FileDistributionService.checkChunkFilename( filename ) ) {
      return new ChunkReader( filename );
    } else if ( FileDistributionService.checkShardFilename( filename ) ) {
      return new ShardReader( filename );
    } else {
      System.err.println(
          "createFileReader: FileReader couldn't be created. "+filename );
      return null;
    }
  }
}
