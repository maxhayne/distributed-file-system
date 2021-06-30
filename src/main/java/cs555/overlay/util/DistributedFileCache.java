package cs555.overlay.util;
import java.util.Collections;
import java.util.Collection;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Set;
import java.util.Map;

public class DistributedFileCache {

	private Map<String,DistributedFile> filecache;

	public DistributedFileCache() {
		this.filecache = new TreeMap<String,DistributedFile>();
	}

	public synchronized String[] getFileList() {
		int size = filecache.size();
		if (size == 0) return null;
		String[] list = new String[size];
		Set<String> keys = filecache.keySet();
		int index = 0;
		for (String key : keys) {
			list[index] = key;
			index++;
		}
		return list;
	}

	public synchronized int getFileSize(String filename) {
		DistributedFile file = filecache.get(filename);
		if (file == null) return 0;
		if (file.chunks == null) return 0;
		return file.chunks.size();
	}

	public synchronized String getChunkStorageInfo(String filename, int sequence) {
		DistributedFile file = filecache.get(filename);
		if (file == null)
			return "|";
		String info = "";
		if (file.chunks.get(sequence) != null && file.chunks.get(sequence).size() != 0) {
			boolean added = false;
			Vector<Chunk> chunks = file.chunks.get(sequence);
			for (Chunk chunk : chunks) {
				if (chunk.corrupt == false) { // only return healthy chunks
					info += chunk.serveridentifier + ",";
					added = true;
				}
			}
			if (added) // remove last comma
				info = info.substring(0,info.length()-1);
		}
		info += "|"; // divides chunks from shards
		if (file.shards.get(sequence) != null && file.shards.get(sequence).size() != 0) {
			Vector<Shard> shards = file.shards.get(sequence);
			for (int i = 0; i < 9; i++) {
				boolean found = false;
				for (Shard shard : shards) {
					info += shard.serveridentifier + ",";
					found = true;
					break;
				}
				if (!found)
					info += String.valueOf(-1) + ",";
			}
			info = info.substring(0,info.length()-1);
		}
		file = null;
		return info;
	}

	// Returns all the chunks removed so that new homes can be found for them.
	public synchronized Set<Chunk> removeAllFilesAtServer(int identifier) {
		Set<Chunk> totalRemoved = new TreeSet<Chunk>(); // remove duplicates if there are any
		Vector<Chunk> removeChunks = new Vector<Chunk>();
		Vector<Shard> removeShards = new Vector<Shard>();
		Vector<String> removeFiles = new Vector<String>();
		Collection<DistributedFile> files = filecache.values();
		for (DistributedFile file : files) {
			// Iterate through all chunks and shards of the file and remove any that are stored on
			// on the server with the identifier
			if (file.chunks != null && file.chunks.size() != 0) {
				Collection<Vector<Chunk>> chunkVectors = file.chunks.values();
				for (Vector<Chunk> chunkVector : chunkVectors) {
					for (Chunk chunk : chunkVector) {
						// Iterating through chunks
						if (chunk.serveridentifier == identifier) {
							removeChunks.add(chunk);
							totalRemoved.add(chunk);
						}
					}
					for (Chunk chunk : removeChunks) {
						chunkVector.removeElement(chunk);
					}
					removeChunks.clear();
				}
			}
			if(file.shards != null && file.shards.size() != 0) {
				Collection<Vector<Shard>> shardVectors = file.shards.values();
				for (Vector<Shard> shardVector : shardVectors) {
					for (Shard shard : shardVector) {
						if (shard.serveridentifier == identifier)
							removeShards.add(shard);
					}
					for (Shard shard : removeShards) {
						shardVector.removeElement(shard);
					}
					removeShards.clear();
				}
			}
			if ((file.chunks == null || file.chunks.size() == 0) 
				&& (file.shards == null || file.shards.size() == 0))
				removeFiles.add(file.filename);
		}
		for (String file : removeFiles) {
			removeFile(file);
		}
		removeFiles.clear();
		removeChunks = null;
		removeShards = null;
		removeFiles = null;
		return totalRemoved;
	}

	public synchronized int fileCount() {
		return filecache.size();
	}

	public synchronized void addFile(String filename) {
		if (filecache.containsKey(filename))
			return;
		DistributedFile newFile = new DistributedFile(filename);
		filecache.put(filename,newFile);
	}

	public synchronized void removeFile(String filename) {
		if (!filecache.containsKey(filename))
			return;
		filecache.get(filename).clear();
		filecache.remove(filename);
	}

	public synchronized void addChunk(Chunk chunk) {
		if (!filecache.containsKey(chunk.filename)) // add file if not present
			this.addFile(chunk.filename);
		DistributedFile file = filecache.get(chunk.filename);
		if (!file.chunks.containsKey(chunk.sequence))
			file.chunks.put(chunk.sequence,new Vector<Chunk>());
		file.chunks.get(chunk.sequence).addElement(chunk);
	}

	public synchronized void removeChunk(Chunk chunk) {
		if (!filecache.containsKey(chunk.filename))
			return;
		DistributedFile file = filecache.get(chunk.filename);
		if(!file.chunks.containsKey(chunk.sequence))
			return;
		file.chunks.get(chunk.sequence).removeElement(chunk); // remove chunk if it exists
	}

	public synchronized void addShard(Shard shard) {
		if (!filecache.containsKey(shard.filename))
			this.addFile(shard.filename);
		DistributedFile file = filecache.get(shard.filename);
		if (!file.shards.containsKey(shard.sequence)) {
			file.shards.put(shard.sequence,new Vector<Shard>());
		}
		file.shards.get(shard.sequence).addElement(shard);
	}

	public synchronized void removeShard(Shard shard) {
		if (!filecache.containsKey(shard.filename))
			return;
		DistributedFile file = filecache.get(shard.filename);
		if (!file.shards.containsKey(shard.sequence))
			return;
		file.shards.get(shard.sequence).removeElement(shard); // remove shard if it exists
	}

	public synchronized void clear() {
		Collection<DistributedFile> files = filecache.values();
		for (DistributedFile file : files)
			file.clear();
		filecache.clear();
	}

	public synchronized boolean containsChunk(Chunk chunk) {
		if (!filecache.containsKey(chunk.filename) 
			|| !filecache.get(chunk.filename).chunks.containsKey(chunk.sequence) 
			|| !filecache.get(chunk.filename).chunks.get(chunk.sequence).contains(chunk)) {
			return false;
		}
		return true;
	}

	public synchronized boolean containsShard(Shard shard) {
		if (!filecache.containsKey(shard.filename) 
			|| !filecache.get(shard.filename).shards.containsKey(shard.sequence) 
			|| !filecache.get(shard.filename).shards.get(shard.sequence).contains(shard)) {
			return false;
		}
		return true;
	}

	public synchronized void markChunkCorrupt(String filename, int sequence, int serveridentifier) {
		if (filecache.containsKey(filename)
			&& filecache.get(filename).chunks.containsKey(sequence)
			&& filecache.get(filename).chunks.get(sequence) != null) {
			Vector<Chunk> vector = filecache.get(filename).chunks.get(sequence);
			for (Chunk chunk : vector) {
				if (chunk.serveridentifier == serveridentifier)
					chunk.corrupt = true;
			}
		}
	}

	public synchronized void markShardCorrupt(String filename, int sequence, int shardnumber, int serveridentifier) {
		if (filecache.containsKey(filename)
			&& filecache.get(filename).shards.containsKey(sequence)
			&& filecache.get(filename).shards.get(sequence) != null) {
			Vector<Shard> vector = filecache.get(filename).shards.get(sequence);
			for (Shard shard : vector) {
				if (shard.serveridentifier == serveridentifier && shard.shardnumber == shardnumber)
					shard.corrupt = true;
			}
		}
	}

	public synchronized void markChunkHealthy(String filename, int sequence, int serveridentifier) {
		if (filecache.containsKey(filename)
			&& filecache.get(filename).chunks.containsKey(sequence)
			&& filecache.get(filename).chunks.get(sequence) != null) {
			Vector<Chunk> vector = filecache.get(filename).chunks.get(sequence);
			for (Chunk chunk : vector) {
				if (chunk.serveridentifier == serveridentifier)
					chunk.corrupt = false;
			}
		}
	}

	public synchronized void markShardHealthy(String filename, int sequence, int shardnumber, int serveridentifier) {
		if (filecache.containsKey(filename)
			&& filecache.get(filename).shards.containsKey(sequence)
			&& filecache.get(filename).shards.get(sequence) != null) {
			Vector<Shard> vector = filecache.get(filename).shards.get(sequence);
			for (Shard shard : vector) {
				if (shard.serveridentifier == serveridentifier && shard.shardnumber == shardnumber)
					shard.corrupt = false;
			}
		}
	}

	// Function to return differences between two DistributedFileCaches.
	// Will return all chunks or shards that are stored in this.filecache
	// that are not stored in the the passed filecache. Will help to 
	// identify chunkservers that are down.
	public synchronized Vector<String> differences(DistributedFileCache passedcache) {
		Vector<String> differences = new Vector<String>();
		for (Map.Entry<String,DistributedFile> entry1 : filecache.entrySet()) {
			String filename = entry1.getKey();
			// entry1.getKey() will be the filename
			// entry1.getValue() will be the DistributedFile instance
			// Have to iterate through entry1.getValue().chunkservers.entrySet()
			for (Map.Entry<Integer,Vector<Chunk>> entry2 : entry1.getValue().chunks.entrySet()) {
				int sequence = entry2.getKey();
				// entry2.getKey() will be the chunk#
				// entry2.getValue() will be the Vector<Chunk> for the chunk#
				// entry2.getValue().contains(Chunk) will tell you if the Chunk is present
				for (Chunk chunk : entry2.getValue()) {
					if (!passedcache.containsChunk(chunk)) {
						differences.add("chunk" + "," + filename + "," + sequence  + "," + chunk.version + "," + chunk.serveridentifier);
					}
				}
			}
			for (Map.Entry<Integer,Vector<Shard>> entry2 : entry1.getValue().shards.entrySet()) {
				int sequence = entry2.getKey();
				for (Shard shard : entry2.getValue()) {
					if (!passedcache.containsShard(shard)) {
						differences.add("shard" + "," + filename + "," + sequence  + "," + shard.shardnumber + "," + shard.serveridentifier);
						//differences.add(filename + "," + sequence + ",shard," + shard.serveridentifier);
					}
				}
			}
		}
		return differences;
	}
}