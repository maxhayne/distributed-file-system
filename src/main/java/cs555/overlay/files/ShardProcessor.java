package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileUtilities;
import cs555.overlay.util.FilenameUtilities;

import java.security.NoSuchAlgorithmException;

public class ShardProcessor {
  private final byte[] fileBytes;
  private final byte[] content;
  private final boolean corrupt;

  /**
   * Processes the byte[] of a shard read from disk, determines if it is
   * corrupt, and if not, cleans it of its hash and metadata.
   *
   * @param fileBytes raw shard byte[] read off disk
   */
  public ShardProcessor(byte[] fileBytes) throws NoSuchAlgorithmException {
    this.fileBytes = fileBytes;
    this.corrupt = FileUtilities.checkShardForCorruption(fileBytes);
    if (!corrupt) {
      this.content = FileUtilities.getDataFromShard(
          FileUtilities.removeHashFromShard(fileBytes));
    } else {
      this.content = null;
    }
  }

  /**
   * Attempts to reconstruct a shard using the other shards of a chunk. If it
   * can, updates the metadata, and packages up the content with new metadata
   * and hash.
   *
   * @param md metadata associated with the shard
   * @param repairShards chunk shards used for reconstruction
   */
  public ShardProcessor(FileMetadata md, byte[][] repairShards)
      throws NoSuchAlgorithmException {
    if (repairShards != null) {
      byte[][] reconstructedShards =
          FileUtilities.decodeMissingShards(repairShards);
      if (reconstructedShards != null) {
        int sequence = FilenameUtilities.getSequence(md.getFilename());
        int fragment = FilenameUtilities.getFragment(md.getFilename());
        md.updateIfWritten();
        this.content = repairShards[fragment];
        this.fileBytes = FileUtilities.readyShardForStorage(sequence, fragment,
            md.getVersion(), md.getTimestamp(), content);
        this.corrupt = false;
        return;
      }
    }
    this.fileBytes = null;
    this.content = null;
    this.corrupt = true;
  }

  public byte[] getBytes() {
    return fileBytes;
  }

  public byte[] getContent() {
    return content;
  }

  public boolean isCorrupt() {
    return corrupt;
  }
}
