package cs555.overlay.util;

public class Shard {

	public String filename;
	public int sequence;
	public int shardnumber;
	public int serveridentifier;
	public boolean corrupt;

	public Shard(String filename, int sequence, int shardnumber, int serveridentifier, boolean corrupt) {
		this.filename = filename;
		this.sequence = sequence;
		this.shardnumber = shardnumber;
		this.serveridentifier = serveridentifier;
		this.corrupt = corrupt;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Shard))
            return false;

       	Shard shard = (Shard) o;

		if (!this.filename.equals(shard.filename)
			|| this.sequence != shard.sequence
			|| this.shardnumber != shard.shardnumber
			|| this.serveridentifier != shard.serveridentifier)
			return false;
		return true;
	}

	public String print() {
		String returnable = "";
		returnable += "Filename: " + filename + '\n';
		returnable += "Sequence: " + sequence + '\n';
		returnable += "ShardNumber: " + shardnumber + '\n';
		returnable += "ServerIdentifier: " + serveridentifier + '\n';
		return returnable;
	}
}