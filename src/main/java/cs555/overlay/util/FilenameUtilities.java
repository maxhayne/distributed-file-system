package cs555.overlay.util;

public class FilenameUtilities {
  public static boolean checkChunkFilename(String filename) {
    boolean matches = filename.matches( ".*_chunk(0|[1-9][0-9]*)*$" );
    String[] split = filename.split( "_chunk" );
    return matches && split.length == 2;
  }

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

  public static int getSequence(String filename) {
    String sequence = filename.split( "_chunk" )[1];
    if ( sequence.contains( "_shard" ) ) {
      sequence = sequence.split( "_shard" )[0];
    }
    return Integer.parseInt( sequence );
  }

  public static int getFragment(String filename) {
    return Integer.parseInt( filename.split( "_shard" )[1] );
  }

}
