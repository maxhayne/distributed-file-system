package cs555.overlay.util;
import cs555.overlay.util.Chunk;
import cs555.overlay.util.Shard;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map;

public class DistributedFile {
	
	public String filename;
	public Map<Integer,Vector<Chunk>> chunks; // sequence number maps to vector of servers which store replications
	public Map<Integer,Vector<Shard>> shards; // sequence number maps to vector of servers which store shards

	public DistributedFile(String filename) {
		this.filename = filename;
		this.chunks = new TreeMap<Integer,Vector<Chunk>>();
		this.shards = new TreeMap<Integer,Vector<Shard>>();
	}

	public void clear() {
		for ( Vector<Chunk> chunkVector : chunks.values() ) {
			if (chunkVector != null) {
				chunkVector.clear();
			}
		}
		for ( Vector<Shard> shardVector : shards.values() ) {
			if (shardVector != null) {
				shardVector.clear();
			}
		}
		chunks.clear();
		shards.clear();
	}
}