package cs555.overlay.files;

import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.FileMetadata;

import java.nio.ByteBuffer;

/**
 * Class used to simplify the reading of Shards off of the disk. Instead of
 * doing everything manually, an instance of this class can be instantiated with
 * the desired filename, and the reading and error-checking can be performed
 * automatically.
 *
 * @author hayne
 */
public class ShardReader implements FileReader {

  private final String filename;
  private byte[] shardBytes;
  private boolean corrupt;
  private FileMetadata metadata;

  public ShardReader(String filename) {
    this.filename = filename;
  }

  /**
   * Attempts to read the shard off the disk. If the shard is corrupt, the
   * member 'corrupt' is set to true. Otherwise, 'corrupt' is set to false, and
   * the shard's data and metadata is extracted.
   *
   * @param fileService the ChunkServer is using to synchronize file reads
   * across threads
   */
  @Override
  public void readAndProcess(FileDistributionService fileService) {
    shardBytes = fileService.readNBytesFromFile( filename,
        FileDistributionService.SHARD_FILE_LENGTH );
    checkForCorruption();
    if ( !corrupt ) {
      shardBytes = FileDistributionService.removeHashFromShard( shardBytes );
      readMetadata();
      shardBytes = FileDistributionService.getDataFromShard( shardBytes );
    }

  }

  /**
   * Checks the shard that has been read off the disk for corruption, and sets
   * the member 'corrupt' accordingly.
   */
  private void checkForCorruption() {
    corrupt = FileDistributionService.checkShardForCorruption( shardBytes );
  }

  private void readMetadata() {
    ByteBuffer shardBuffer = ByteBuffer.wrap( shardBytes );
    metadata = new FileMetadata( filename, shardBuffer.getInt( 8 ),
        shardBuffer.getLong( 12 ) );
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
  public FileMetadata getMetadata() {
    return metadata;
  }

  @Override
  public byte[] getData() {
    return shardBytes;
  }
}