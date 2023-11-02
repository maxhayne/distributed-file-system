package cs555.overlay.util;

import erasure.ReedSolomon;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Class used by the ChunkServer to synchronize file accesses across threads.
 * Also contains some utility functions for working with chunks/shards.
 *
 * @author hayne
 */
public class FileSynchronizer {

  public static int CHUNK_DATA_LENGTH = 65536;
  public static int CHUNK_FILE_LENGTH = 65720;
  public static int SHARD_FILE_LENGTH =
      10924+20+(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG;

  private final Path directory; // directory the server is storing files into

  /**
   * Constructor. Takes the identifier of the ChunkServer using the
   * FileSynchronizer, and creates a (hopefully) unique folder in the /tmp
   * directory for the ChunkServer to use.
   *
   * @param identifier of ChunkServer using the FileSynchronizer
   * @throws IOException if the can't be created
   */
  public FileSynchronizer(int identifier) throws IOException {
    this.directory =
        Paths.get( File.separator, "tmp", "ChunkServer"+"-"+identifier );
    Files.createDirectories( directory );
  }

  /**
   * Generates a 20 byte hash of a byte array using SHA1.
   *
   * @param array byte[] to generate hash of
   * @return 20 byte hash of array
   * @throws NoSuchAlgorithmException if SHA1 isn't available
   */
  public static byte[] SHA1FromBytes(byte[] array)
      throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance( "SHA1" );
    return digest.digest( array );
  }

  /**
   * Transforms a byte[] of length CHUNK_DATA_LENGTH (2^16) into shards. Does
   * not assert that the length of content is 2^16!
   *
   * @param length of actual content within the 2^8 byte array
   * @param content content of file
   * @return byte[TOTAL_SHARDS][] of shards
   */
  public static byte[][] makeShardsFromContent(int length, byte[] content) {
    int storedSize = Constants.BYTES_IN_INT+Constants.CHUNK_DATA_LENGTH;
    int shardSize = (storedSize+8)/Constants.DATA_SHARDS;
    //System.out.println( shardSize );
    int bufferSize = shardSize*Constants.DATA_SHARDS;
    byte[] allBytes = new byte[bufferSize];
    ByteBuffer allBytesBuffer = ByteBuffer.wrap( allBytes );
    //System.out.println( "makeShardsFromContent length: "+length );
    allBytesBuffer.putInt( length );
    allBytesBuffer.put( content );
    byte[][] shards = new byte[Constants.TOTAL_SHARDS][shardSize];
    for ( int i = 0; i < Constants.DATA_SHARDS; ++i ) {
      System.arraycopy( allBytes, i*shardSize, shards[i], 0, shardSize );
    }
    ReedSolomon rs =
        new ReedSolomon( Constants.DATA_SHARDS, Constants.PARITY_SHARDS );
    rs.encodeParity( shards, 0, shardSize );
    return shards;
  }

  /**
   * Takes a byte[][] of shards, any of which could be null, and attempts to
   * decode them into the chunk they were made from.
   *
   * @param shards byte[][] shards to be decoded
   * @return the byte[] that was originally encoded into shards, null if the
   * data couldn't be reconstructed
   */
  public static byte[][] decodeMissingShards(byte[][] shards) {
    if ( shards.length != Constants.TOTAL_SHARDS ) {
      return null;
    }
    boolean[] shardPresent = new boolean[Constants.TOTAL_SHARDS];
    int shardCount = 0;
    int shardSize = 0;
    for ( int i = 0; i < Constants.TOTAL_SHARDS; i++ ) {
      if ( shards[i] != null ) {
        shardPresent[i] = true;
        shardCount++;
        shardSize = shards[i].length;
      }
    }
    if ( shardCount < Constants.DATA_SHARDS ) {
      return null;
    }
    for ( int i = 0; i < Constants.TOTAL_SHARDS; i++ ) {
      if ( !shardPresent[i] ) {
        shards[i] = new byte[shardSize];
      }
    }
    ReedSolomon rs =
        new ReedSolomon( Constants.DATA_SHARDS, Constants.PARITY_SHARDS );
    rs.decodeMissing( shards, shardPresent, 0, shardSize );
    return shards;
  }

  /**
   * Once shards have been decoded, the first Constants.DATA_SHARDS shards
   * contain the data of the encoded chunk, prepended with a four byte integer
   * of the chunk's length. A new byte[] of that length is created, filled with
   * the chunk's data, and returned.
   *
   * @param shards decoded shards
   * @return byte[] of chunk's data
   */
  public static byte[] getContentFromShards(byte[][] shards) {
    int shardSize = shards[0].length;
    byte[] decodedChunk = new byte[shardSize*Constants.DATA_SHARDS];
    for ( int i = 0; i < Constants.DATA_SHARDS; i++ ) {
      System.arraycopy( shards[i], 0, decodedChunk, shardSize*i, shardSize );
    }
    // first four bytes contains the length (hopefully)
    int length = ByteBuffer.wrap( decodedChunk ).getInt();
    return Arrays.copyOfRange( decodedChunk, 4, 4+length );
    //return Arrays.copyOfRange( decodedChunk, 4, 65724 );
  }

  /**
   * Takes the sequence and version numbers of a chunk, along with its content,
   * and transforms it into a byte[] that is ready to be written to disk.
   *
   * @param sequence sequence number of chunk
   * @param version version number of chunk
   * @param content byte[] of chunk's content
   * @return byte[] of chunk, ready to be written to disk
   */
  public static byte[] readyChunkForStorage(int sequence, int version,
      byte[] content) {
    int contentRemaining = content.length;
    byte[] chunkToFileArray = new byte[65720]; // total size of stored chunk
    byte[] sliceArray = new byte[8195];
    ByteBuffer chunkToFileBuffer = ByteBuffer.wrap( chunkToFileArray );
    ByteBuffer sliceBuffer = ByteBuffer.wrap( sliceArray );
    sliceBuffer.putInt( 0 ); // padding
    sliceBuffer.putInt( sequence );
    sliceBuffer.putInt( version );
    sliceBuffer.putInt( contentRemaining );
    sliceBuffer.putLong( System.currentTimeMillis() );
    int position = 0;
    if ( contentRemaining >= 8195-24 ) {
      sliceBuffer.put( content, position, 8195-24 );
      contentRemaining -= (8195-24);
      position += (8195-24);
    } else {
      sliceBuffer.put( content, 0, contentRemaining );
      contentRemaining = 0;
    }
    try {
      byte[] hash = SHA1FromBytes( sliceArray );
      chunkToFileBuffer.put( hash );
      chunkToFileBuffer.put( sliceArray );
      sliceBuffer.clear();
      Arrays.fill( sliceArray, ( byte ) 0 );
      for ( int i = 0; i < 7; i++ ) {
        if ( contentRemaining == 0 ) {
          hash = SHA1FromBytes( sliceArray );
        } else if ( contentRemaining < 8195 ) {
          sliceBuffer.put( content, position, contentRemaining );
          contentRemaining = 0;
          hash = SHA1FromBytes( sliceArray );
        } else {
          sliceBuffer.put( content, position, 8195 );
          contentRemaining -= 8195;
          position += 8195;
          hash = SHA1FromBytes( sliceArray );
        }
        chunkToFileBuffer.put( hash );
        chunkToFileBuffer.put( sliceArray );
        sliceBuffer.clear();
        Arrays.fill( sliceArray, ( byte ) 0 );
      }
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println(
          "readyChunkForStorage: Can't access algorithm for SHA1." );
      return null;
    }
    return chunkToFileArray;
  }

  /**
   * Combines the sequence, fragment, and version numbers of a shard, along with
   * its byte[] content into a new byte[] that is ready to be written to disk.
   *
   * @param sequence number of chunk that has been encoded into shards
   * @param fragment fragment number of shard (0-8)
   * @param version version number of file
   * @param content byte[] of encoded shard
   * @return byte[] of shard, ready to be written to disk
   */
  public static byte[] readyShardForStorage(int sequence, int fragment,
      int version, byte[] content) {
    byte[] shardToFileArray =
        new byte[20+(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG+
                 10924]; // Hash+Sequence
    // +Fragment+Version+Timestamp+Data, 10964 bytes in total
    byte[] shardWithMetaData =
        new byte[(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG+10924];
    ByteBuffer shardMetaWrap = ByteBuffer.wrap( shardWithMetaData );
    shardMetaWrap.putInt( sequence );
    shardMetaWrap.putInt( fragment );
    shardMetaWrap.putInt( version );
    shardMetaWrap.putLong( System.currentTimeMillis() );
    shardMetaWrap.put( content );
    try {
      byte[] hash = SHA1FromBytes( shardWithMetaData );
      ByteBuffer shardFileArrayWrap = ByteBuffer.wrap( shardToFileArray );
      shardFileArrayWrap.put( hash );
      shardFileArrayWrap.put( shardWithMetaData );
      return shardToFileArray;
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println(
          "readyShardForStorage: Can't access algorithm for SHA1." );
      return null;
    }
  }

  /**
   * Checks the byte[] of a chunk read from the disk, and returns an int[]
   * containing the indices of the slices that are corrupt.
   *
   * @param chunkBytes byte[] of chunk read from disk
   * @return int[] of slice indices that are corrupt for this chunk
   */
  public static ArrayList<Integer> checkChunkForCorruption(byte[] chunkBytes) {
    ArrayList<Integer> corrupt = new ArrayList<>();
    for ( int i = 0; i < 8; ++i ) {
      corrupt.add( i );
    }
    if ( chunkBytes == null ) {
      return corrupt;
    }
    ByteBuffer chunk = ByteBuffer.wrap( chunkBytes );
    byte[] hash = new byte[20];
    byte[] slice = new byte[8195];
    try {
      for ( int i = 0; i < 8; i++ ) {
        chunk.get( hash );
        chunk.get( slice );
        byte[] computedHash = SHA1FromBytes( slice );
        if ( Arrays.equals( hash, computedHash ) ) {
          corrupt.remove( Integer.valueOf( i ) );
        }
        Arrays.fill( hash, ( byte ) 0 );
        Arrays.fill( slice, ( byte ) 0 );
      }
    } catch ( BufferUnderflowException bue ) {
      // The array wasn't the correct length for a chunk
      System.err.println(
          "checkChunkCorruption: Byte string wasn't "+"formatted properly." );
      //return null;
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println( "checkChunkCorruption: Couldn't use SHA1." );
      //return null;
    }
    // The array could pass all the tests and still be corrupt, if any
    // information was added to the end of the file storing the chunk.
    // Correct length of the file is 65720 bytes.
    return corrupt;
  }

  /**
   * Checks if a shard is corrupt.
   *
   * @param shardBytes byte[] of shard read from disk
   * @return true if corrupt, false if not
   */
  public static boolean checkShardForCorruption(byte[] shardBytes) {
    if ( shardBytes == null ) {
      return true;
    }
    ByteBuffer shardArrayBuffer = ByteBuffer.wrap( shardBytes );
    byte[] hash = new byte[20];
    byte[] shard =
        new byte[(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG+10924];
    try {
      shardArrayBuffer.get( hash );
      shardArrayBuffer.get( shard );
      byte[] computedHash = SHA1FromBytes( shard );
      if ( Arrays.equals( hash, computedHash ) ) {
        return false;
      }
    } catch ( BufferUnderflowException bue ) {
      return true;
    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println( "checkShardForCorruption: Couldn't use SHA1." );
      return true;
    }
    return true;
  }

  /**
   * Removes the hashes from a chunk that has been read from the disk. The
   * hashes are spliced into the file at regular intervals.
   *
   * @param chunkArray byte[] to be cleaned
   * @return a new byte[] with hashes removed
   */
  public static byte[] removeHashesFromChunk(byte[] chunkArray) {
    ByteBuffer chunk = ByteBuffer.wrap( chunkArray );
    byte[] cleanedChunk = new byte[65560];
    for ( int i = 0; i < 8; i++ ) {
      chunk.position( chunk.position()+20 );
      chunk.get( cleanedChunk, i*8195, 8195 );
    }
    return cleanedChunk;
    // cleanedChunk will start like this:
    // Padding (0 int)
    // Sequence (int)
    // Version (int)
    // Total bytes of data in this chunk (int)
    // Timestamp (long)
  }

  /**
   * Removes the hash from the shard (the first 20 bytes).
   *
   * @param shardArray byte[] to be cleaned
   * @return a new byte[] with hash removed
   */
  public static byte[] removeHashFromShard(byte[] shardArray) {
    ByteBuffer shard = ByteBuffer.wrap( shardArray );
    byte[] cleanedShard =
        new byte[(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG+10924];
    shard.position( shard.position()+20 );
    shard.get( cleanedShard, 0, cleanedShard.length );
    return cleanedShard;
    // cleanedShard will start like this:
    // Sequence (int)
    // Fragment (int)
    // Version (int)
    // Timestamp (long)
    // Data
  }

  /**
   * Removes metadata from chunk, and returns only its content. Assumes hashes
   * have already been removed.
   *
   * @param chunkArray byte[] to be cleaned of metadata
   * @return a new byte[] with metadata removed
   */
  public static byte[] getDataFromChunk(byte[] chunkArray) {
    ByteBuffer chunk = ByteBuffer.wrap( chunkArray );
    int chunkLength = chunk.getInt( 12 );
    chunk.position( 24 );
    byte[] data = new byte[chunkLength];
    chunk.get( data );
    return data;
  }

  /**
   * Removes metadata from a shard that has already has its hash removed.
   *
   * @param shardArray byte[] to be cleaned of metadata
   * @return a new byte[] with metadata removed
   */
  public static byte[] getDataFromShard(byte[] shardArray) {
    ByteBuffer shard = ByteBuffer.wrap( shardArray );
    shard.position( 20 );
    byte[] data = new byte[10924];
    shard.get( data );
    return data;
  }

  /**
   * Generates a path to a filename that will be stored at the ChunkServer this
   * FileSynchronizer instance is resident on.
   *
   * @param filename filename of file
   * @return path to filename
   */
  public Path getPath(String filename) {
    return directory.resolve( filename );
  }

  /**
   * Returns usable space at the directory this ChunkServer is using.
   *
   * @return usable space in bytes
   */
  public long getUsableSpace() {
    return (new File( directory.toString() )).getUsableSpace();
  }

  /**
   * Returns an array of filename strings that are either chunks or shards that
   * are stored in the folder pointed to by the 'directory' member.
   *
   * @return String[] of filenames in 'directory'
   * @throws IOException if the directory that Files.list() is to be run on
   * doesn't exist
   */
  public String[] listFiles() throws IOException {
    try ( Stream<Path> stream = Files.list( directory ) ) {
      List<String> filenames =
          stream.filter( file -> !Files.isDirectory( file ) )
                .map( Path::getFileName )
                .map( Path::toString )
                .filter( file -> FilenameUtilities.checkChunkFilename( file ) ||
                                 FilenameUtilities.checkShardFilename( file ) )
                .toList();
      String[] filenameArray = new String[filenames.size()];
      for ( int i = 0; i < filenames.size(); ++i ) {
        filenameArray[i] = filenames.get( i );
      }
      return filenameArray;
    }
  }

  public synchronized void truncateFile(String filename, long size) {
    String filePath = getPath( filename ).toString();
    try ( RandomAccessFile file = new RandomAccessFile( filePath, "rw" );
          FileChannel channel = file.getChannel();
          FileLock lock = channel.lock() ) {
      channel.truncate( size );
    } catch ( IOException ioe ) {
      System.out.println(
          "truncateFile: Error truncating '"+filename+"'. "+ioe.getMessage() );
    }
  }

  /**
   * Reads up to the first N bytes of a file.
   *
   * @param filename name of file to read
   * @param N bytes of file will be read
   * @return byte string of length N, filled until an exception is thrown or the
   * entire file has been read
   */
  public synchronized byte[] readNBytesFromFile(String filename, int N) {
    String fileWithPath = directory.resolve( filename ).toString();
    byte[] fileBytes = new byte[N];
    try ( RandomAccessFile file = new RandomAccessFile( fileWithPath, "r" );
          FileChannel channel = file.getChannel();
          FileLock lock = channel.lock( 0, N, true ) ) {
      file.read( fileBytes );
    } catch ( IOException ioe ) {
      System.err.println(
          "readNBytesFromFile: Unable to read "+N+" bytes of '"+filename+"'. "+
          ioe.getMessage() );
    }
    return fileBytes;
  }

  /**
   * Writes a new file, or replaces one if it already exists.
   *
   * @param filename the name of the file to write
   * @param content to be written
   * @return true if write succeeded, false if it didn't
   */
  public synchronized boolean overwriteFile(String filename, byte[] content) {
    String filenameWithPath = directory.resolve( filename ).toString();
    try (
        RandomAccessFile file = new RandomAccessFile( filenameWithPath, "rw" );
        FileChannel channel = file.getChannel();
        FileLock lock = channel.lock() ) {
      channel.truncate( 0 );
      file.write( content );
      System.out.println(
          "overwriteFile: "+content.length+" bytes "+"written to '"+filename+
          "'" );
      return true;
    } catch ( IOException ioe ) {
      return false;
    }
  }

  /**
   * Deletes a file from the server of a particular name. If the name is neither
   * a specific chunk nor specific shard, looks for files with that basename,
   * and deletes those.
   *
   * @param filename String filename to be deleted
   * @throws IOException if the deletion or listFiles() operation fails
   */
  public synchronized void deleteFile(String filename) throws IOException {
    // if filename is a specific chunk or shard, try to delete it
    if ( FilenameUtilities.checkChunkFilename( filename ) ||
         FilenameUtilities.checkShardFilename( filename ) ) {
      Files.deleteIfExists( getPath( filename ) );
      return;
    }
    // if filename is a base (what would come before "_chunk" or
    // "_shard"), then delete all chunks or shards with that base
    String[] files = listFiles();
    for ( String file : files ) {
      if ( file.split( "_chunk" )[0].equals( filename ) ) {
        Files.deleteIfExists( getPath( file ) );
      }
    }
  }
}