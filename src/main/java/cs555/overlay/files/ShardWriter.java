package cs555.overlay.files;

import cs555.overlay.util.FileSynchronizer;

import java.security.NoSuchAlgorithmException;

public class ShardWriter implements FileWriter {
  private final String filename;
  private byte[] content;

  private byte[] preparedShard;

  private byte[][] reconstructionShards;

  /**
   * Constructor for if your goal is to write a new shard file.
   *
   * @param filename of shard to be written
   * @param content of shard to be written (raw data)
   */
  public ShardWriter(String filename, byte[] content) {
    this.filename = filename;
    this.content = content;
  }

  /**
   * Constructor for if your goal is to write a shard reconstructed from other
   * shards. Shards used to reconstruct are added later.
   *
   * @param reader used to read a previously stored shard
   */
  public ShardWriter(FileReader reader) {
    this.filename = reader.getFilename();
  }

  /**
   * Prepares either a totally new shard, or one that has been reconstructed
   * from replacementShards[][] (which shouldn't be null if content is null).
   *
   * @throws NoSuchAlgorithmException if SHA1 can't be used
   */
  @Override
  public void prepare() throws NoSuchAlgorithmException {
    if ( content != null ) {
      prepareNewShard();
    } else {
      prepareShard();
    }
  }

  /**
   * Prepares a new shard based on the content byte string.
   */
  private void prepareNewShard() {
    int sequence = getSequenceFromFilename();
    int fragment = getFragmentFromFilename();
    preparedShard =
        FileSynchronizer.readyShardForStorage( sequence, fragment, 0,
            content );
  }

  /**
   * Prepares a new shard by reconstructing it from an array of other shards.
   */
  private void prepareShard() {
    if ( reconstructionShards != null ) {
      byte[][] reconstructedShards =
          FileSynchronizer.decodeMissingShards( reconstructionShards );
      if ( reconstructedShards != null ) {
        int sequence = getSequenceFromFilename();
        int fragment = getFragmentFromFilename();
        preparedShard =
            FileSynchronizer.readyShardForStorage( sequence, fragment, 0,
                reconstructedShards[fragment] );
      }
    }
  }

  /**
   * Parses the filename for its sequence number.
   *
   * @return sequence number of this shard
   */
  private int getSequenceFromFilename() {
    return Integer.parseInt(
        filename.split( "_shard" )[0].split( "_chunk" )[1] );
  }

  /**
   * Parses the filename for its fragment number.
   *
   * @return fragment number of this shard
   */
  private int getFragmentFromFilename() {
    return Integer.parseInt( filename.split( "_shard" )[1] );
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
    if ( preparedShard != null ) {
      return synchronizer.overwriteFile( filename, preparedShard );
    }
    return false;
  }


  @Override
  public String getFilename() {
    return filename;
  }
}
