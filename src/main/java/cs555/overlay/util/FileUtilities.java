package cs555.overlay.util;

import cs555.overlay.config.Constants;
import erasure.ReedSolomon;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Holds utility functions for working with Chunks and Shards.
 *
 * @author hayne
 */
public class FileUtilities {
  public static int CHUNK_FILE_LENGTH = 65720;
  public static int SHARD_FILE_LENGTH =
      10924 + 20 + (3*Constants.BYTES_IN_INT) + Constants.BYTES_IN_LONG;

  /**
   * Generates a 20 byte hash of a byte array using SHA1.
   *
   * @param array byte[] to generate hash of
   * @return 20 byte hash of array
   * @throws NoSuchAlgorithmException if SHA1 isn't available
   */
  public static byte[] SHA1FromBytes(byte[] array)
      throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("SHA1").digest(array);
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
    int storedSize = Constants.BYTES_IN_INT + Constants.CHUNK_DATA_LENGTH;
    int shardSize = (storedSize + 8)/Constants.DATA_SHARDS;
    int bufferSize = shardSize*Constants.DATA_SHARDS;
    byte[] allBytes = new byte[bufferSize];
    ByteBuffer buf = ByteBuffer.wrap(allBytes);
    buf.putInt(length);
    buf.put(content);
    byte[][] shards = new byte[Constants.TOTAL_SHARDS][shardSize];
    for (int i = 0; i < Constants.DATA_SHARDS; ++i) {
      System.arraycopy(allBytes, i*shardSize, shards[i], 0, shardSize);
    }
    ReedSolomon rs =
        new ReedSolomon(Constants.DATA_SHARDS, Constants.PARITY_SHARDS);
    rs.encodeParity(shards, 0, shardSize);
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
    if (shards.length != Constants.TOTAL_SHARDS) {
      return null;
    }
    boolean[] shardPresent = new boolean[Constants.TOTAL_SHARDS];
    int shardCount = 0;
    int shardSize = 0;
    for (int i = 0; i < Constants.TOTAL_SHARDS; i++) {
      if (shards[i] != null) {
        shardPresent[i] = true;
        shardCount++;
        shardSize = shards[i].length;
      }
    }
    if (shardCount < Constants.DATA_SHARDS) {
      return null;
    }
    for (int i = 0; i < Constants.TOTAL_SHARDS; i++) {
      if (!shardPresent[i]) {
        shards[i] = new byte[shardSize];
      }
    }
    ReedSolomon rs =
        new ReedSolomon(Constants.DATA_SHARDS, Constants.PARITY_SHARDS);
    rs.decodeMissing(shards, shardPresent, 0, shardSize);
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
    int shardLen = shards[0].length;
    byte[] decodedChunk = new byte[shardLen*Constants.DATA_SHARDS];
    for (int i = 0; i < Constants.DATA_SHARDS; i++) {
      System.arraycopy(shards[i], 0, decodedChunk, shardLen*i, shardLen);
    }
    // first four bytes contains the length (hopefully)
    int length = ByteBuffer.wrap(decodedChunk).getInt();
    return Arrays.copyOfRange(decodedChunk, 4, 4 + length);
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
      long timestamp, byte[] content) throws NoSuchAlgorithmException {
    int contentRemaining = content.length;
    byte[] chunkFile = new byte[CHUNK_FILE_LENGTH];
    byte[] sliceArray = new byte[8195];
    ByteBuffer fileBuf = ByteBuffer.wrap(chunkFile);
    ByteBuffer sliceBuf = ByteBuffer.wrap(sliceArray);
    sliceBuf.putInt(0); // padding
    sliceBuf.putInt(sequence);
    sliceBuf.putInt(version);
    sliceBuf.putInt(contentRemaining);
    sliceBuf.putLong(timestamp);
    int position = 0;
    if (contentRemaining >= 8195 - 24) {
      sliceBuf.put(content, position, 8195 - 24);
      contentRemaining -= (8195 - 24);
      position += (8195 - 24);
    } else {
      sliceBuf.put(content, 0, contentRemaining);
      contentRemaining = 0;
    }
    byte[] hash = SHA1FromBytes(sliceArray);
    fileBuf.put(hash);
    fileBuf.put(sliceArray);
    sliceBuf.clear();
    Arrays.fill(sliceArray, (byte) 0);
    for (int i = 0; i < 7; i++) {
      if (contentRemaining == 0) {
        hash = SHA1FromBytes(sliceArray);
      } else if (contentRemaining < 8195) {
        sliceBuf.put(content, position, contentRemaining);
        contentRemaining = 0;
        hash = SHA1FromBytes(sliceArray);
      } else {
        sliceBuf.put(content, position, 8195);
        contentRemaining -= 8195;
        position += 8195;
        hash = SHA1FromBytes(sliceArray);
      }
      fileBuf.put(hash);
      fileBuf.put(sliceArray);
      sliceBuf.clear();
      Arrays.fill(sliceArray, (byte) 0);
    }
    return chunkFile;
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
      int version, long timestamp, byte[] content)
      throws NoSuchAlgorithmException {
    byte[] shardWithMeta = new byte[SHARD_FILE_LENGTH - 20];
    ByteBuffer buf = ByteBuffer.wrap(shardWithMeta);
    buf.putInt(sequence);
    buf.putInt(fragment);
    buf.putInt(version);
    buf.putLong(timestamp);
    buf.put(content);

    byte[] shardFile = new byte[SHARD_FILE_LENGTH];
    ByteBuffer fileBuf = ByteBuffer.wrap(shardFile);
    fileBuf.put(SHA1FromBytes(shardWithMeta));
    fileBuf.put(shardWithMeta);
    return shardFile;
  }

  /**
   * Checks the byte[] of a chunk read from the disk, and returns an int[]
   * containing the indices of the slices that are corrupt.
   *
   * @param chunkBytes byte[] of chunk read from disk
   * @return int[] of slice indices that are corrupt for this chunk
   */
  public static List<Integer> checkChunkForCorruption(byte[] chunkBytes)
      throws NoSuchAlgorithmException {
    List<Integer> corruptIndices = new ArrayList<>(8);
    for (int i = 0; i < 8; ++i) {
      corruptIndices.add(i);
    }
    if (chunkBytes == null) {
      return corruptIndices;
    }
    byte[] hash = new byte[20];
    byte[] slice = new byte[8195];
    ByteBuffer buf = ByteBuffer.wrap(chunkBytes);
    try {
      for (int i = 0; i < 8; i++) {
        buf.get(hash);
        buf.get(slice);
        if (Arrays.equals(hash, SHA1FromBytes(slice))) {
          corruptIndices.remove(Integer.valueOf(i));
        }
        Arrays.fill(hash, (byte) 0);
        Arrays.fill(slice, (byte) 0);
      }
    } catch (BufferUnderflowException e) {
      // chunkBytes is too short, corruptIndices still okay to return
    }
    // If chunkBytes is too long, will still pass all the tests.
    return corruptIndices;
  }

  /**
   * Checks if a shard is corrupt.
   *
   * @param shardBytes raw byte[] of shard from disk
   * @return true if corrupt, false if not
   */
  public static boolean checkShardForCorruption(byte[] shardBytes)
      throws NoSuchAlgorithmException {
    if (shardBytes == null) {
      return true;
    }
    ByteBuffer buf = ByteBuffer.wrap(shardBytes);
    byte[] hash = new byte[20];
    byte[] shard = new byte[SHARD_FILE_LENGTH - 20];
    try {
      buf.get(hash);
      buf.get(shard);
      if (Arrays.equals(hash, SHA1FromBytes(shard))) {
        return false;
      }
    } catch (BufferUnderflowException bue) {
      // shardBytes is too short, it is corrupt
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
    ByteBuffer buf = ByteBuffer.wrap(chunkArray);
    byte[] hashlessChunk = new byte[65560];
    for (int i = 0; i < 8; i++) {
      buf.position(buf.position() + 20);
      buf.get(hashlessChunk, i*8195, 8195);
    }
    return hashlessChunk;
    // hashlessChunk will start like this:
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
    byte[] hashlessShard = new byte[SHARD_FILE_LENGTH - 20];
    ByteBuffer.wrap(shardArray).get(20, hashlessShard, 0, hashlessShard.length);
    return hashlessShard;
    // hashlessShard will start like this:
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
   * @param chunkArray byte[] to be cleaned of hash
   * @return a new byte[] with metadata removed
   */
  public static byte[] getDataFromChunk(byte[] chunkArray) {
    ByteBuffer buf = ByteBuffer.wrap(chunkArray);
    int chunkLength = buf.getInt(12);
    byte[] chunkData = new byte[chunkLength];
    buf.get(24, chunkData, 0, chunkData.length);
    return chunkData;
  }

  /**
   * Removes metadata from a shard that has already has its hash removed.
   *
   * @param shardArray byte[] to be cleaned of metadata
   * @return a new byte[] with metadata removed
   */
  public static byte[] getDataFromShard(byte[] shardArray) {
    byte[] shardData = new byte[10924];
    ByteBuffer.wrap(shardArray).get(20, shardData, 0, shardData.length);
    return shardData;
  }
}