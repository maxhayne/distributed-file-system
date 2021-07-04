package cs555.overlay.util;

public class Chunk extends ServerFile implements Comparable<Chunk> {

	public String filename;
	public int sequence;
	public int version;
	public int serveridentifier;
	public boolean corrupt;
	public long created;

	public Chunk(String filename, int sequence, int version, int serveridentifier, boolean corrupt) {
		this.filename = filename;
		this.sequence = sequence;
		this.version = version;
		this.serveridentifier = serveridentifier;
		this.corrupt = corrupt;
		this.created = System.currentTimeMillis();
	}

	@Override
	public int compareTo(Chunk chunk) {
		if (this.filename.compareTo(chunk.filename) == 0) {
			if (this.sequence < chunk.sequence) {
				return -1;
			} else if (this.sequence > chunk.sequence){
				return 1;
			} else {
				if (this.version < chunk.version) {
					return -1;
				} else if (this.version > chunk.version) {
					return 1;
				} else {
					if (this.serveridentifier < chunk.serveridentifier) {
						return -1;
					} else if (this.serveridentifier > chunk.serveridentifier) {
						return 1;
					}
					return 0;
				}
			}
		}
		return this.filename.compareTo(chunk.filename);
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Chunk))
            return false;

       	Chunk chunk = (Chunk) o;
       	// May need to modify this to ignore the version number,as a
       	// different version number doesn't imply different content.
		if (!this.filename.equals(chunk.filename)
			|| this.sequence != chunk.sequence
			//|| this.version != chunk.version
			|| this.serveridentifier != chunk.serveridentifier)
			return false;
		return true;
	}

	public String print() {
		String returnable = "";
		returnable += "Filename: " + filename + '\n';
		returnable += "Sequence: " + sequence + '\n';
		returnable += "Version: " + version + '\n';
		returnable += "ServerIdentifier: " + serveridentifier + '\n';
		returnable += "Corrupt Status: " + corrupt + '\n'; 
		return returnable;
	}

	public String getType() {
		return "CHUNK";
	}
}