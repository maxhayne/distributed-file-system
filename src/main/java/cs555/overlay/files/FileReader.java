package cs555.overlay.files;

import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.FileMetadata;

/**
 * An interface that both ChunkReader and ShardReader implement. This way,
 * files can be read off the disk without knowing whether they are chunks or
 * shards, and their corruption data can be extracted accordingly.
 *
 * @author hayne
 */
public interface FileReader {
  public void readAndProcess(FileDistributionService fileService);

  public boolean isCorrupt();

  public int[] getCorruption();

  public FileMetadata getMetadata();

  public byte[] getData();
}
