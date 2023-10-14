package cs555.overlay.util;

public class Shard extends ServerFile implements Comparable<Shard> {

    public byte type;

    public String filename;
    public int sequence;
    public int fragment;
    public int version;
    public long timestamp;

    public int serverIdentifier;
    public boolean corrupt;

    public Shard( String filename, int sequence, int fragment, int version,
        long timestamp, int serverIdentifier, boolean corrupt ) {
        this.type = Constants.SHARD_TYPE;
        this.filename = filename;
        this.sequence = sequence;
        this.fragment = fragment;
        this.timestamp = timestamp;
        this.serverIdentifier = serverIdentifier;
        this.corrupt = corrupt;
    }

    @Override
    public int compareTo( Shard shard ) {
        if ( this.filename.compareTo( shard.filename ) == 0 ) {
            if ( this.sequence < shard.sequence ) {
                return -1;
            } else if ( this.sequence > shard.sequence ) {
                return 1;
            } else {
                if ( this.fragment < shard.fragment ) {
                    return -1;
                } else if ( this.fragment > shard.fragment ) {
                    return 1;
                } else {
                    if ( this.serverIdentifier < shard.serverIdentifier ) {
                        return -1;
                    } else if ( this.serverIdentifier
                                > shard.serverIdentifier ) {
                        return 1;
                    }
                    return 0;
                }
            }
        }
        return this.filename.compareTo( shard.filename );
    }

    @Override
    public boolean equals( Object o ) {
        if ( o == this ) {
            return true;
        }
        if ( !( o instanceof Shard ) ) {
            return false;
        }

        Shard shard = ( Shard ) o;

        if ( !this.filename.equals( shard.filename )
             || this.sequence != shard.sequence
             || this.fragment != shard.fragment
             || this.serverIdentifier != shard.serverIdentifier ) {
            return false;
        }
        return true;
    }

    public String print() {
        String returnable = "";
        returnable += "Filename: " + filename + '\n';
        returnable += "Sequence: " + sequence + '\n';
        returnable += "Fragment: " + fragment + '\n';
        returnable += "Version: " + version + '\n';
        returnable += "Timestamp: " + timestamp + '\n';
        returnable += "ServerIdentifier: " + serverIdentifier + '\n';
        returnable += "Corrupt: " + corrupt + '\n';
        return returnable;
    }

    public byte getType() {
        return type;
    }
}