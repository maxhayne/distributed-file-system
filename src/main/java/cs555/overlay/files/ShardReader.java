package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileSynchronizer;

import java.nio.ByteBuffer;

/**
 * Class used to simplify the reading of Shards from the disk. Instead of doing
 * everything manually, an instance of this class can be instantiated with the
 * desired filename, and the reading and error-checking can be performed
 * automatically.
 *
 * @author hayne
 */
public class ShardReader implements FileReader {

  private final String filename;
  private byte[] shardBytes;
  private boolean corrupt;
  public ShardReader(String filename) {
    this.filename = filename;
  }

  /**
   * Attempts to read the shard from the disk. If the shard is corrupt, the
   * member 'corrupt' is set to true. Otherwise, 'corrupt' is set to false, and
   * the shard's data and metadata is extracted.
   *
   * @param synchronizer the ChunkServer is using to synchronize file reads
   * across threads
   */
  @Override
  public void readAndProcess(FileSynchronizer synchronizer) {
    shardBytes = synchronizer.readNBytesFromFile( filename,
        FileSynchronizer.SHARD_FILE_LENGTH );
    checkForCorruption();
    if ( !corrupt ) {
      shardBytes = FileSynchronizer.removeHashFromShard( shardBytes );
      shardBytes = FileSynchronizer.getDataFromShard( shardBytes );
    }

  }

  /**
   * Checks the shard that has been read off the disk for corruption, and sets
   * the member 'corrupt' accordingly.
   */
  private void checkForCorruption() {
    corrupt = FileSynchronizer.checkShardForCorruption( shardBytes );
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public boolean isCorrupt() {
    return corrupt;
  }

  @Override
  public int[] getCorruption() {
    return null;
  }

  @Override
  public byte[] getData() {
    return shardBytes;
  }
}