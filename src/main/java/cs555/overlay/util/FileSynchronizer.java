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

public class FileSynchronizer {

  public static int CHUNK_DATA_LENGTH = 65536;
  public static int SHA1_LENGTH = 20;
  public static int CHUNK_FILE_LENGTH = 65720;
  public static int SHARD_FILE_LENGTH =
      10924+20+(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG;

  private final Path directory;

  public FileSynchronizer(int identifier) throws IOException {
    this.directory =
        Paths.get( File.separator, "tmp", "ChunkServer"+"-"+identifier );
    Files.createDirectories( directory );
  }

  // Function for generating hash
  public static byte[] SHA1FromBytes(byte[] data)
      throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance( "SHA1" );
    return digest.digest( data );
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
    System.out.println( shardSize );
    int bufferSize = shardSize*Constants.DATA_SHARDS;
    byte[] allBytes = new byte[bufferSize];
    ByteBuffer allBytesBuffer = ByteBuffer.wrap( allBytes );
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

  public static byte[] getChunkFromShards(byte[][] shards) {
    int shardSize = shards[0].length;
    byte[] decodedChunk = new byte[shardSize*Constants.DATA_SHARDS];
    for ( int i = 0; i < Constants.DATA_SHARDS; i++ ) {
      System.arraycopy( shards[i], 0, decodedChunk, shardSize*i, shardSize );
    }
    return Arrays.copyOfRange( decodedChunk, 4,
        65724 ); // Will then have to removeHashesFromChunk
    // (correctedDecode) and getDataFromChunk()
  }

  public static synchronized byte[] getNextChunkFromFile(String filename,
      int sequence) {
    File tryFile = new File( filename );
    if ( !tryFile.isFile() ) {
      return null;
    }
    int position = sequence*65536;
    try ( RandomAccessFile file = new RandomAccessFile( tryFile, "r" );
          FileChannel channel = file.getChannel();
          FileLock lock = channel.lock( position, 65536, true ) ) {
      if ( position > channel.size() ) {
        return null;
      }
      byte[] data = (channel.size()-position) < 65536 ?
                        new byte[( int ) channel.size()-position] :
                        new byte[65536];
      ByteBuffer buffer = ByteBuffer.wrap( data );
      int read = 1;
      while ( buffer.hasRemaining() && read > 0 ) {
        read = channel.read( buffer, position );
        position += read;
      }
      return data;
    } catch ( IOException ioe ) {
      return null;
    }
  }

  /**
   * Takes a filename and data, and creates a byte[] which is ready to be
   * written to disk as a file.
   *
   * @param filename of file to be stored
   * @param data byte[] of chunk or shard
   * @return byte[] of file data, null if unsuccessful
   */
  public static byte[] readyFileForStorage(String filename, byte[] data) {
    byte[] fileBytes;
    if ( FilenameUtilities.checkChunkFilename( filename ) ) {
      int sequence = Integer.parseInt( filename.split( "_chunk" )[1] );
      fileBytes = readyChunkForStorage( sequence, 0, data );
    } else if ( FilenameUtilities.checkShardFilename( filename ) ) {
      String[] split = filename.split( "_shard" );
      int fragment = Integer.parseInt( split[1] );
      int sequence = Integer.parseInt( split[0].split( "_chunk" )[1] );
      fileBytes = readyShardForStorage( sequence, fragment, 0, data );
    } else {
      System.out.println( "readyFileForStorage: '"+filename+
                          "' is neither a chunk nor a shard. It cannot be "+
                          "converted into a byte[] for storage." );
      return null;
    }
    return fileBytes;
  }

  // Takes chunk data, combines with metadata and hashes, basically
  // prepares it for writing to a file.
  public static byte[] readyChunkForStorage(int sequence, int version,
      byte[] chunkArray) {
    int chunkArrayRemaining = chunkArray.length;
    byte[] chunkToFileArray = new byte[65720]; // total size of stored chunk
    byte[] sliceArray = new byte[8195];
    ByteBuffer chunkToFileBuffer = ByteBuffer.wrap( chunkToFileArray );
    ByteBuffer sliceBuffer = ByteBuffer.wrap( sliceArray );
    sliceBuffer.putInt( 0 ); // padding
    sliceBuffer.putInt( sequence );
    sliceBuffer.putInt( version );
    sliceBuffer.putInt( chunkArrayRemaining );
    sliceBuffer.putLong( System.currentTimeMillis() );
    int position = 0;
    if ( chunkArrayRemaining >= 8195-24 ) {
      sliceBuffer.put( chunkArray, position, 8195-24 );
      chunkArrayRemaining -= (8195-24);
      position += (8195-24);
    } else {
      sliceBuffer.put( chunkArray, 0, chunkArrayRemaining );
      chunkArrayRemaining = 0;
    }
    try {
      byte[] hash = SHA1FromBytes( sliceArray );
      chunkToFileBuffer.put( hash );
      chunkToFileBuffer.put( sliceArray );
      sliceBuffer.clear();
      Arrays.fill( sliceArray, ( byte ) 0 );
      for ( int i = 0; i < 7; i++ ) {
        if ( chunkArrayRemaining == 0 ) {
          hash = SHA1FromBytes( sliceArray );
        } else if ( chunkArrayRemaining < 8195 ) {
          sliceBuffer.put( chunkArray, position, chunkArrayRemaining );
          chunkArrayRemaining = 0;
          hash = SHA1FromBytes( sliceArray );
        } else {
          sliceBuffer.put( chunkArray, position, 8195 );
          chunkArrayRemaining -= 8195;
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

  public static byte[] readyShardForStorage(int sequence, int fragment,
      int version, byte[] shardArray) {
    byte[] shardToFileArray =
        new byte[20+(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG+
                 10924]; // Hash+Sequence
    // +Fragment+Version+Timestamp+Data, 10994 bytes in total
    byte[] shardWithMetaData =
        new byte[(3*Constants.BYTES_IN_INT)+Constants.BYTES_IN_LONG+10924];
    ByteBuffer shardMetaWrap = ByteBuffer.wrap( shardWithMetaData );
    shardMetaWrap.putInt( sequence );
    shardMetaWrap.putInt( fragment );
    shardMetaWrap.putInt( version );
    shardMetaWrap.putLong( System.currentTimeMillis() );
    shardMetaWrap.put( shardArray );
    byte[] hash;
    try {
      hash = SHA1FromBytes( shardWithMetaData );
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

  // Check chunk for errors and return integer array containing slice numbers
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

  // Removes hashes from chunk
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

  // Removes hash from shard
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

  // Removes metadata, and strips padding from end of array
  public static byte[] getDataFromChunk(byte[] chunkArray) {
    ByteBuffer chunk = ByteBuffer.wrap( chunkArray );
    int chunkLength = chunk.getInt( 12 );
    chunk.position( 24 );
    byte[] data = new byte[chunkLength];
    chunk.get( data );
    return data;
  }

  // Removes metadata from shard
  public static byte[] getDataFromShard(byte[] shardArray) {
    ByteBuffer shard = ByteBuffer.wrap( shardArray );
    shard.position( 20 );
    byte[] data = new byte[10924];
    shard.get( data );
    return data;
  }

  // Create file if it doesn't exist, append file with data
  public static synchronized boolean appendFile(String filename, byte[] data) {
    File tryFile = new File( filename );
    if ( !tryFile.isFile() ) {
      return false;
    }
    try ( RandomAccessFile file = new RandomAccessFile( tryFile, "rw" );
          FileChannel channel = file.getChannel();
          FileLock lock = channel.lock() ) {
      channel.position( channel.size() );
      ByteBuffer buffer = ByteBuffer.wrap( data );
      while ( buffer.hasRemaining() ) {
        channel.write( buffer );
      }
      return true;
    } catch ( IOException ioe ) {
      return false;
    }
  }

  public Path getPath(String filename) {
    return directory.resolve( filename );
  }

  public long getUsableSpace() {
    return (new File( directory.toString() )).getUsableSpace();
  }

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

  // Read any file and return a byte[] of the data
  public synchronized byte[] readBytesFromFile(String filename) {
    byte[] fileBytes;
    try {
      fileBytes = Files.readAllBytes( getPath( filename ) );
    } catch ( IOException ioe ) {
      System.out.println( "readBytesFromFile: Unable to read '"+filename+"'."+
                          ioe.getMessage() );
      return null;
    }
    return fileBytes;
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

  // Create file if it doesn't exist, if it does exist, return false
  public synchronized boolean writeNewFile(String filename, byte[] data) {
    File tryFile = new File( filename );
    if ( tryFile.isFile() ) {
      return false;
    }
    try ( RandomAccessFile file = new RandomAccessFile( tryFile, "rw" );
          FileChannel channel = file.getChannel();
          FileLock lock = channel.lock() ) {
      ByteBuffer buffer = ByteBuffer.wrap( data );
      while ( buffer.hasRemaining() ) {
        channel.write( buffer );
      }
      return true;
    } catch ( IOException ioe ) {
      return false;
    }
  }

  // Write new file, replace if it already exists.
  public synchronized boolean overwriteFile(String filename, byte[] data) {
    String filenameWithPath = directory.resolve( filename ).toString();
    try (
        RandomAccessFile file = new RandomAccessFile( filenameWithPath, "rw" );
        FileChannel channel = file.getChannel();
        FileLock lock = channel.lock() ) {
      channel.truncate( 0 );
      ByteBuffer buffer = ByteBuffer.wrap( data );
      while ( buffer.hasRemaining() ) {
        int bytesWritten = channel.write( buffer );
        System.out.println(
            "overwriteFile: "+bytesWritten+" bytes "+"written to '"+filename+
            "'" );
      }
      return true;
    } catch ( IOException ioe ) {
      return false;
    }
  }

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

  // Replace slices with new slices
  public synchronized boolean replaceSlices(String filename, int[] slices,
      byte[][] sliceData) {
    File tryFile = new File( filename );
    if ( !tryFile.isFile() ) {
      return false; // can't replace slices for file that doesn't exist
    }
    try ( RandomAccessFile file = new RandomAccessFile( tryFile, "rw" );
          FileChannel channel = file.getChannel();
          FileLock lock = channel.lock() ) {
      int numSlices = slices.length;
      for ( int i = 0; i < numSlices; i++ ) {
        int position = 8215*slices[i]; // includes the hashes
        ByteBuffer newSlice = ByteBuffer.wrap( sliceData[i] );
        while ( newSlice.hasRemaining() ) {
          position += channel.write( newSlice, position );
        }
      }
      return true;
    } catch ( IOException ioe ) {
      return false;
    }
  }

  // Replace slices with new slices
  public byte[][] getSlices(byte[] chunkArray, int[] slices) {
    int numSlices = slices.length;
    byte[][] sliceData = new byte[numSlices][8195];
    for ( int i = 0; i < numSlices; i++ ) {
      ByteBuffer buffer = ByteBuffer.wrap( sliceData[i] );
      buffer.put( chunkArray, (20*(slices[i]+1))+slices[i]*8195, 8195 );
    }
    return sliceData;
  }
}
