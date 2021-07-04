package cs555.overlay.util;

public class Shard extends ServerFile implements Comparable<Shard> {

	public String filename;
	public int sequence;
	public int shardnumber;
	public int serveridentifier;
	public boolean corrupt;
	public long created;

	public Shard(String filename, int sequence, int shardnumber, int serveridentifier, boolean corrupt) {
		this.filename = filename;
		this.sequence = sequence;
		this.shardnumber = shardnumber;
		this.serveridentifier = serveridentifier;
		this.corrupt = corrupt;
		this.created = System.currentTimeMillis();
	}

	@Override
	public int compareTo(Shard shard) {
		if (this.filename.compareTo(shard.filename) == 0) {
			if (this.sequence < shard.sequence) {
				return -1;
			} else if (this.sequence > shard.sequence){
				return 1;
			} else {
				if (this.shardnumber < shard.shardnumber) {
					return -1;
				} else if (this.shardnumber > shard.shardnumber) {
					return 1;
				} else {
					if (this.serveridentifier < shard.serveridentifier) {
						return -1;
					} else if (this.serveridentifier > shard.serveridentifier) {
						return 1;
					}
					return 0;
				}
			}
		}
		return this.filename.compareTo(shard.filename);
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

	public String getType() {
		return "SHARD";
	}
}