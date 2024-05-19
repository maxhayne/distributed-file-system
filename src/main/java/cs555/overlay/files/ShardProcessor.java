package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileUtilities;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RequestChunk;

import java.security.NoSuchAlgorithmException;

public class ShardProcessor implements FileProcessor {

  private byte[] fileBytes;
  private byte[] content;
  private boolean corrupt;

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
   * Attempts to reconstruct the corrupt shard using
   *
   * @param md metadata associated with the shard
   * @param repairShards shards to be used to make the repair
   * @return true if repaired, false if not
   * @throws NoSuchAlgorithmException if SHA-1 isn't available
   */
  public boolean repair(FileMetadata md, byte[][] repairShards)
      throws NoSuchAlgorithmException {
    if (!corrupt) {
      return false; // repair not necessary
    }
    if (repairShards != null) {
      byte[][] reconstructedShards =
          FileUtilities.decodeMissingShards(repairShards);
      if (reconstructedShards != null) {
        int sequence = FilenameUtilities.getSequence(md.getFilename());
        int fragment = FilenameUtilities.getFragment(md.getFilename());
        md.updateIfWritten();
        content = repairShards[fragment];
        fileBytes = FileUtilities.readyShardForStorage(sequence, fragment,
            md.getVersion(), md.getTimestamp(), content);
        corrupt = false;
        return true;
      }
    }
    return false;
  }

  /**
   * Attach the fragment to the request message if it's not corrupted.
   *
   * @param request RequestChunk message
   */
  public void attachToRequest(RequestChunk request) {
    if (!corrupt) {
      int index = FilenameUtilities.getFragment(request.getFilenameAtServer());
      request.getPieces()[index] = content;
    }
  }

  /**
   * Attach the fragment to the repair message if it's not corrupted.
   *
   * @param repair RepairChunk message
   */
  public void attachToRepair(RepairChunk repair) {
    if (!corrupt) {
      int index = FilenameUtilities.getFragment(repair.getFilenameAtServer());
      repair.getPieces()[index] = content;
    }
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
