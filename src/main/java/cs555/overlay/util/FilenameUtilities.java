package cs555.overlay.util;

/**
 * Functions for working with filenames with "_chunk#" and "_shard#" attached.
 *
 * @author hayne
 */
public class FilenameUtilities {

  /**
   * Checks if a filename is properly formatted for a shard.
   *
   * @param filename to be checked
   * @return true if it is properly formatted, false otherwise
   */
  public static boolean checkChunkFilename(String filename) {
    boolean matches = filename.matches( ".*_chunk(0|[1-9][0-9]*)*$" );
    String[] split = filename.split( "_chunk" );
    return matches && split.length == 2;
  }

  /**
   * Checks if a filename is properly formatted for a chunk.
   *
   * @param filename to be checked
   * @return true if it is properly formatted, false otherwise
   */
  public static boolean checkShardFilename(String filename) {
    boolean matches = filename.matches( ".*_chunk(0|[1-9][0-9]*)_shard[0-8]$" );
    String[] split1 = filename.split( "_chunk" );
    String[] split2 = filename.split( "_shard" );
    return matches && split1.length == 2 && split2.length == 2;
  }

  /**
   * Returns the base of the filename.
   *
   * @param filename to be parsed
   * @return name of the file before "_chunk", or the unchanged filename if it
   * doesn't contain "_chunk"
   */
  public static String getBaseFilename(String filename) {
    if ( filename.contains( "_chunk" ) ) {
      return filename.split( "_chunk" )[0];
    }
    return filename;
  }

  /**
   * Gets the sequence number from the filename of a properly formatted
   * chunk/shard.
   *
   * @param filename to be parsed
   * @return integer sequence number
   */
  public static int getSequence(String filename) {
    String sequence = filename.split( "_chunk" )[1];
    if ( sequence.contains( "_shard" ) ) {
      sequence = sequence.split( "_shard" )[0];
    }
    return Integer.parseInt( sequence );
  }

  /**
   * Gets the fragment number from the filename of a properly formatted shard.
   *
   * @param filename to be parsed
   * @return integer fragment number
   */
  public static int getFragment(String filename) {
    return Integer.parseInt( filename.split( "_shard" )[1] );
  }

}
