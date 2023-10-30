package cs555.overlay.files;

import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileSynchronizer;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Class used to simplify the reading of Chunks from the disk. Instead of doing
 * everything manually, an instance of this class can be instantiated with the
 * desired filename, and the reading and error-checking can be performed
 * automatically.
 *
 * @author hayne
 */
public class ChunkReader implements FileReader {

  private final String filename;
  private byte[] chunkBytes;
  private boolean corrupt;
  private FileMetadata metadata;
  private int[] corruptSlices;
  private byte[][] slices; // slices include SHA1

  public ChunkReader(String filename) {
    this.filename = filename;
  }

  /**
   * Attempts to read the file specified by the member 'filename'. If the read
   * is successful and the Chunk isn't corrupt, the metadata is read and the
   * chunk's data is extracted. If the Chunk is corrupt, 'corrupt' is set to
   * true and 'corruptSlices' is populated.
   *
   * @param synchronizer the ChunkServer is using to synchronize file reads
   * across threads
   */
  @Override
  public void readAndProcess(FileSynchronizer synchronizer) {
    chunkBytes = synchronizer.readNBytesFromFile( filename,
        FileSynchronizer.CHUNK_FILE_LENGTH );
    populateSlices();
    checkForCorruption();
    readMetadata();
    if ( !corrupt ) {
      chunkBytes = FileSynchronizer.removeHashesFromChunk( chunkBytes );
      chunkBytes = FileSynchronizer.getDataFromChunk( chunkBytes );
    } else {
      for ( int corruptSlice : corruptSlices ) {
        System.out.println( corruptSlice );
      }
    }
  }

  /**
   * Checks what was read from the disk for corruption. Sets 'corrupt'
   * accordingly, and creates 'corruptSlices' array to contains indices of
   * slices that are corrupt.
   */
  private void checkForCorruption() {
    ArrayList<Integer> corruptions =
        FileSynchronizer.checkChunkForCorruption( chunkBytes );
    if ( corruptions.isEmpty() ) {
      corrupt = false;
      System.out.println( "There are no corruptions!" );
    } else {
      corrupt = true;
      corruptSlices = ArrayUtilities.arrayListToArray( corruptions );
    }
  }

  /**
   * Is called after the Chunk is read from the disk, and splits what was read
   * into slices of the appropriate size. Keeping a copy of the slices will be
   * necessary for making repairs.
   */
  private void populateSlices() {
    slices = new byte[8][20+8195];
    for ( int i = 0; i < 8; ++i ) {
      System.arraycopy( chunkBytes, i*(20+8195), slices[i], 0, 20+8195 );
    }
  }

  /**
   * Attempts to read the version and timestamp for the Chunk if the slice
   * holding that information (first slice) isn't corrupt.
   */
  private void readMetadata() {
    boolean firstSliceCorrupt = false;
    if ( corruptSlices != null ) {
      for ( int corruptSlice : corruptSlices ) {
        if ( corruptSlice == 0 ) {
          firstSliceCorrupt = true;
          break;
        }
      }
    }
    if ( !firstSliceCorrupt ) {
      ByteBuffer chunkBuffer = ByteBuffer.wrap( slices[0] );
      metadata = new FileMetadata( filename, chunkBuffer.getInt( 20+8 ),
          chunkBuffer.getInt( 20+16 ) );
    }
  }

  /**
   * Getter for the raw slices read off the disk.
   *
   * @return slices that have been read off the disk
   */
  public byte[][] getSlices() {
    return slices;
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
    return corruptSlices;
  }

  @Override
  public FileMetadata getMetadata() {
    return metadata;
  }

  @Override
  public byte[] getData() {
    return chunkBytes;
  }
}
