package cs555.overlay.util;

public class FileMetadata {

    public String filename; // will include _chunk and/or _shard
    public int version;
    public long timestamp;

    public FileMetadata( String filename, int version, long timestamp ) {
        this.filename = filename;
        this.version = version;
        this.timestamp = timestamp;
    }

    // 0 for chunk, 1 for shard
    public int getType() {
        if ( checkShardFilename( filename ) ) {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals( Object o ) {
        if ( o == this ) {
            return true;
        }
        if ( !( o instanceof FileMetadata ) ) {
            return false;
        }

        FileMetadata filedata = ( FileMetadata ) o;
        // May need to modify this to ignore the version number,as a
        // different version number doesn't imply different content.
        if ( !this.filename.equals( filedata.filename )
             || this.version != filedata.version ) {
            return false;
        }
        return true;
    }

    private boolean checkShardFilename( String filename ) {
        boolean matches = filename.matches( ".*_chunk[0-9]*_shard[0-8]$" );
        String[] split1 = filename.split( "_chunk" );
        String[] split2 = filename.split( "_shard" );
        if ( matches && split1.length == 2 && split2.length == 2 ) {
            return true;
        }
        return false;
    }
}