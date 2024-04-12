package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileSynchronizer;
import cs555.overlay.util.FilenameUtilities;

import java.security.NoSuchAlgorithmException;

public class ShardWriter implements FileWriter {
  private final FileMetadata metadata;
  private byte[] content;
  private byte[] preparedShard;
  private byte[][] reconstructionShards;

  /**
   * Constructor for if your goal is to write a new shard file.
   *
   * @param metadata of shard to be written
   */
  public ShardWriter(FileMetadata metadata) {
    this.metadata = metadata;
  }

  /**
   * Sets the content of the shard to be written.
   *
   * @param content byte[] of file content
   */
  public void setContent(byte[] content) {
    this.content = content;
  }

  /**
   * Prepares either a totally new shard, or one that has been reconstructed
   * from replacementShards[][] (which shouldn't be null if content is null).
   *
   * @throws NoSuchAlgorithmException if SHA1 can't be used
   */
  @Override
  public void prepare() throws NoSuchAlgorithmException {
    if (content != null) {
      prepareNewShard();
    } else {
      prepareShard();
    }
  }

  /**
   * Prepares a new shard based on the content byte string.
   */
  private void prepareNewShard() {
    int sequence = FilenameUtilities.getSequence(metadata.getFilename());
    int fragment = FilenameUtilities.getFragment(metadata.getFilename());
    preparedShard = FileSynchronizer.readyShardForStorage(sequence, fragment,
        metadata.getVersion(), metadata.getTimestamp(), content);
  }

  /**
   * Prepares a new shard by reconstructing it from an array of other shards.
   */
  private void prepareShard() {
    if (reconstructionShards != null) {
      byte[][] reconstructedShards =
          FileSynchronizer.decodeMissingShards(reconstructionShards);
      if (reconstructedShards != null) {
        int sequence = FilenameUtilities.getSequence(metadata.getFilename());
        int fragment = FilenameUtilities.getFragment(metadata.getFilename());
        preparedShard =
            FileSynchronizer.readyShardForStorage(sequence, fragment,
                metadata.getVersion(), metadata.getTimestamp(),
                reconstructedShards[fragment]);
      }
    }
  }

  /**
   * Setter for reconstructionShards
   *
   * @param reconstructionShards array of byte strings to be used to reconstruct
   * the shard with the fragment number indicated in this ShardWriter's
   * filename.
   */
  public void setReconstructionShards(byte[][] reconstructionShards) {
    this.reconstructionShards = reconstructionShards;
  }

  /**
   * Writes the preparedShard byte string to disk with the name 'filename'.
   *
   * @param synchronizer this ChunkServer is using to synchronize file accesses
   * across threads
   * @return true for success, false for failure
   */
  @Override
  public boolean write(FileSynchronizer synchronizer) {
    if (preparedShard != null) {
      return synchronizer.overwriteFile(metadata.getFilename(), preparedShard);
    }
    return false;
  }


  @Override
  public String getFilename() {
    return metadata.getFilename();
  }
}
