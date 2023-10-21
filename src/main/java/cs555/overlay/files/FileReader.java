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
  String getFilename();

  void readAndProcess(FileDistributionService fileService);

  boolean isCorrupt();

  int[] getCorruption();

  FileMetadata getMetadata();

  byte[] getData();
}
