package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileSynchronizer;
import cs555.overlay.util.FilenameUtilities;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

/**
 * Class used to simplify the preparing and writing of a chunk to disk. Can
 * either write a new chunk to disk based on a byte[] of content, or use a
 * combination of previously-read and newly added slices to fashion a new
 * chunk.
 *
 * @author hayne
 */
public class ChunkWriter implements FileWriter {
  private final FileMetadata metadata;
  private byte[] content;
  private ChunkReader reader;
  private byte[] preparedChunk;
  private int[] slicesToRepair;
  private byte[][] replacementSlices;

  public ChunkWriter(FileMetadata metadata) {
    this.metadata = metadata;
  }

  public ChunkWriter(FileMetadata metadata, FileReader reader) {
    this.metadata = metadata;
    this.reader = (ChunkReader) reader;
  }

  /**
   * Sets the content of the writer.
   *
   * @param content byte[] of file content
   */
  @Override
  public void setContent(byte[] content) {
    this.content = content;
  }

  /**
   * Prepares either a totally new chunk, or an updated version of the one
   * stored in the reader.
   */
  @Override
  public void prepare() throws NoSuchAlgorithmException {
    if (content != null) {
      prepareNewChunk();
    } else {
      prepareChunk();
    }
  }

  /**
   * If 'reader' is null, prepares a chunk for storage based on the content in
   * 'content'.
   */
  private void prepareNewChunk() {
    int sequence = FilenameUtilities.getSequence(metadata.getFilename());
    preparedChunk =
        FileSynchronizer.readyChunkForStorage(sequence, metadata.getVersion(),
            metadata.getTimestamp(), content);
  }

  /**
   * Will only be called if 'reader' was passed in the constructor. Prepares a
   * chunk for storage by replacing slices that were read by the 'reader' with
   * slices in the 'preparedSlices' array.
   *
   * @throws NoSuchAlgorithmException if SHA1 can't be accessed
   */
  private void prepareChunk() throws NoSuchAlgorithmException {
    byte[][] slices = replaceSlices();
    updateMetadata(slices[0]);
    preparedChunk = new byte[FileSynchronizer.CHUNK_FILE_LENGTH];
    for (int i = 0; i < 8; ++i) {
      System.arraycopy(slices[i], 0, preparedChunk, i*(20 + 8195), 20 + 8195);
    }
  }

  /**
   * Gets the slices stored in the reader, and replaces the slices corresponding
   * to the ones indicated in the slicesToRepair array with those in the
   * replacementSlices array.
   *
   * @return the array of byte[] with slices replaced
   */
  private byte[][] replaceSlices() {
    byte[][] slices = reader.getSlices();
    if (slicesToRepair != null && replacementSlices != null &&
        slicesToRepair.length == replacementSlices.length) {
      for (int i = 0; i < slicesToRepair.length; ++i) {
        slices[slicesToRepair[i]] = replacementSlices[i];
      }
    }
    return slices;
  }

  /**
   * Given the first slice of the chunk (assuming the hash hasn't been removed),
   * updates the version and timestamp in the slice. Then recomputes the hash
   * and replaces the old one.
   *
   * @param firstSlice of chunk with updated version and timestamp, and
   * recomputed hash
   * @throws NoSuchAlgorithmException if SHA1 can't be accessed
   */
  private void updateMetadata(byte[] firstSlice)
      throws NoSuchAlgorithmException {
    ByteBuffer firstSliceBuffer = ByteBuffer.wrap(firstSlice);
    updateVersion(firstSliceBuffer);
    updateTimestamp(firstSliceBuffer);

    byte[] updatedSliceData = new byte[firstSlice.length - 20];
    firstSliceBuffer.get(20, updatedSliceData);

    byte[] recomputedHash = FileSynchronizer.SHA1FromBytes(updatedSliceData);

    firstSliceBuffer.put(0, recomputedHash);
  }

  /**
   * Updates the version of the slice with the value in FileMetadata.
   *
   * @param firstSliceBuffer ByteBuffer of first slice of chunk
   */
  private void updateVersion(ByteBuffer firstSliceBuffer) {
    firstSliceBuffer.putInt(28, metadata.getVersion());
  }

  /**
   * Updates the timestamp of the slice.
   *
   * @param firstSliceBuffer ByteBuffer of first slice of chunk
   */
  private void updateTimestamp(ByteBuffer firstSliceBuffer) {
    firstSliceBuffer.putLong(36, metadata.getTimestamp());
  }

  /**
   * Sets slicesToRepair and replacementSlices to those passed as parameters.
   *
   * @param slicesToRepair int[] of slice indices needing repair
   * @param replacementSlices byte[][] of slices which have indices of those
   * indicated in slicesToRepair
   */
  public void setReplacementSlices(int[] slicesToRepair,
      byte[][] replacementSlices) {
    this.slicesToRepair = slicesToRepair;
    this.replacementSlices = replacementSlices;
  }

  /**
   * Writes the 'preparedChunk' to the disk with the name 'filename'.
   *
   * @param synchronizer the ChunkServer is using to synchronize file accesses
   * across this node
   * @return true if write succeeded, false for failure
   */
  @Override
  public boolean write(FileSynchronizer synchronizer) {
    if (preparedChunk != null) {
      return synchronizer.overwriteFile(metadata.getFilename(), preparedChunk);
    }
    return false;
  }

  @Override
  public String getFilename() {
    return metadata.getFilename();
  }
}