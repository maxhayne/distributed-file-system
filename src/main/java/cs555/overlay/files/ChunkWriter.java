package cs555.overlay.files;

import cs555.overlay.util.FileDistributionService;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

public class ChunkWriter implements FileWriter {
  private final String filename;
  private byte[] content;
  private ChunkReader reader;

  private byte[] preparedChunk;

  private int[] slicesToRepair;
  private byte[][] replacementSlices;

  public ChunkWriter(String filename, byte[] content) {
    this.filename = filename;
    this.content = content;
  }

  public ChunkWriter(FileReader reader) {
    this.filename = reader.getFilename();
    this.reader = ( ChunkReader ) reader;
  }

  /**
   * Prepares either a totally new chunk, or an updated version of the one
   * stored in the reader.
   */
  @Override
  public void prepare() throws NoSuchAlgorithmException {
    if ( content != null ) {
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
    int sequence = getSequenceFromFilename();
    preparedChunk =
        FileDistributionService.readyChunkForStorage( sequence, 0, content );
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
    updateMetadata( slices[0] );
    preparedChunk = new byte[FileDistributionService.CHUNK_FILE_LENGTH];
    ByteBuffer preparedChunkBuffer = ByteBuffer.wrap( preparedChunk );
    for ( byte[] slice : slices ) {
      preparedChunkBuffer.put( slice );
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
    if ( slicesToRepair != null && replacementSlices != null &&
         slicesToRepair.length == replacementSlices.length ) {
      for ( int i = 0; i < slicesToRepair.length; ++i ) {
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
    ByteBuffer firstSliceBuffer = ByteBuffer.wrap( firstSlice );
    updateVersion( firstSliceBuffer );
    updateTimestamp( firstSliceBuffer );

    byte[] updatedSliceData = new byte[firstSlice.length-20];
    firstSliceBuffer.get( 20, updatedSliceData );

    byte[] recomputedHash =
        FileDistributionService.SHA1FromBytes( updatedSliceData );

    firstSliceBuffer.put( 0, recomputedHash );
  }

  /**
   * Updates the version of the slice if 'reader' is not null.
   *
   * @param firstSliceBuffer ByteBuffer of first slice of chunk
   */
  private void updateVersion(ByteBuffer firstSliceBuffer) {
    if ( reader.getMetadata() != null ) {
      firstSliceBuffer.putInt( 28, reader.getMetadata().version+1 );
    }
  }

  /**
   * Updates the timestamp of the slice.
   *
   * @param firstSliceBuffer ByteBuffer of first slice of chunk
   */
  private void updateTimestamp(ByteBuffer firstSliceBuffer) {
    firstSliceBuffer.putLong( 36, System.currentTimeMillis() );
  }

  /**
   * Parses the filename of the chunk to find the filename. Assumes the filename
   * is properly formatted.
   *
   * @return sequence number of chunk
   */
  private int getSequenceFromFilename() {
    return Integer.parseInt( filename.split( "_chunk" )[1] );
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
   * @param fileService the ChunkServer is using to synchronize file accesses
   * across this node
   * @return true if write succeeded, false for failure
   */
  @Override
  public boolean write(FileDistributionService fileService) {
    if ( preparedChunk != null ) {
      return fileService.overwriteFile( filename, preparedChunk );
    }
    return false;
  }

  @Override
  public String getFilename() {
    return filename;
  }
}