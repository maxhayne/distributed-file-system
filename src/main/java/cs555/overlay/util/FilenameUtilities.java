package cs555.overlay.util;

import java.util.regex.Pattern;

/**
 * Functions for working with filenames appended with "_chunk#" and "_shard#".
 *
 * @author hayne
 */
public class FilenameUtilities {
  private static final Pattern filenamePattern =
      Pattern.compile( ".*_chunk(0|[1-9][0-9]*)(_shard[0-8])?$" );
  private static final Pattern chunkPattern =
      Pattern.compile( ".*_chunk(0|[1-9][0-9]*)$" );
  private static final Pattern shardPattern =
      Pattern.compile( ".*_chunk(0|[1-9][0-9]*)_shard[0-8]$" );

  /**
   * Check if a filename is
   *
   * @param filename to be checked
   * @return if filename is properly formatted for a chunk or a shard
   */
  public static boolean checkFilename(String filename) {
    return filenamePattern.matcher( filename ).matches();
  }

  /**
   * Checks if a filename is properly formatted for a chunk.
   *
   * @param filename to be checked
   * @return true if it is properly formatted, false otherwise
   */
  public static boolean checkChunkFilename(String filename) {
    return chunkPattern.matcher( filename ).matches();
  }

  /**
   * Checks if a filename is properly formatted for a shard.
   *
   * @param filename to be checked
   * @return true if it is properly formatted, false otherwise
   */
  public static boolean checkShardFilename(String filename) {
    return shardPattern.matcher( filename ).matches();
  }

  /**
   * Returns the base of the filename.
   *
   * @param filename to be parsed
   * @return name of the file before "_chunk", or the unchanged filename if it
   * doesn't contain "_chunk"
   */
  public static String getBaseFilename(String filename) {
    int lastIndex = filename.lastIndexOf( "_chunk" );
    return lastIndex == -1 ? filename : filename.substring( 0, lastIndex );
  }

  /**
   * Gets the sequence number from the filename of a properly formatted
   * chunk/shard.
   *
   * @param filename to be parsed
   * @return integer sequence number
   */
  public static int getSequence(String filename) {
    int lastIndex = filename.lastIndexOf( "_chunk" );
    return Integer.parseInt(
        filename.substring( lastIndex+6 ).split( "_" )[0] );
  }

  /**
   * Gets the fragment number from the filename of a properly formatted shard.
   *
   * @param filename to be parsed
   * @return integer fragment number
   */
  public static int getFragment(String filename) {
    int lastIndex = filename.lastIndexOf( "_shard" );
    return Integer.parseInt( filename.substring( lastIndex+6 ) );
  }

}
