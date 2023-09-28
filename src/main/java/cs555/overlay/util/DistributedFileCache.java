package cs555.overlay.util;

import java.util.Collections;
import java.util.Collection;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Set;
import java.util.Map;

public class DistributedFileCache {

	private Map<String,DistributedFile> fileCache;

	public DistributedFileCache() {
		this.fileCache = new TreeMap<String,DistributedFile>();
	}

	// Remove entries from the fileCache that have no chunks or shards associated with them
	public synchronized void prune() {
		Vector<String> toRemove = new Vector<String>();
		for (Map.Entry<String,DistributedFile> entry : fileCache.entrySet()) {
			boolean empty = true;
			DistributedFile file = entry.getValue();
			if (file.chunks.size() == 0 && file.shards.size() == 0) {
				empty = true;
			} else {
				Collection<Vector<Chunk>> chunkVectorCollection = file.chunks.values();
				for (Vector<Chunk> chunkVector : chunkVectorCollection) {
					if (chunkVector.size() != 0) {
						empty = false;
						break;
					}
				}
				Collection<Vector<Shard>> shardVectorCollection = file.shards.values();
				for (Vector<Shard> shardVector : shardVectorCollection) {
					if (shardVector.size() != 0) {
						empty = false;
						break;
					}
				}
			}
			if (empty) toRemove.add(entry.getKey());
		}
		// Remove the empty distributed files
		for (String filename : toRemove) {
			fileCache.get(filename).clear();
			fileCache.remove(filename);
		}
	}

	public synchronized String[] getFileList() {
		int size = fileCache.size();
		if (size == 0) return null;
		String[] list = new String[size];
		Set<String> keys = fileCache.keySet();
		int index = 0;
		for (String key : keys) {
			list[index] = key;
			index++;
		}
		return list;
	}

	public synchronized int getFileSize(String filename) {
		DistributedFile file = fileCache.get(filename);
		if (file == null) return 0;
		int size;
		if (file.chunks == null && file.shards != null) {
			return file.shards.size();
		} else if (file.chunks != null && file.shards == null) {
			return file.chunks.size();
		} else if (file.shards == null && file.chunks == null) {
			return 0;
		} else {
			return file.shards.size() >= file.chunks.size() ? file.shards.size() : file.chunks.size();
		}
	}

	public synchronized String getChunkStorageInfo(String filename, int sequence) {
		DistributedFile file = fileCache.get(filename);
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
			if (added) info = info.substring(0,info.length()-1);
		}
		info += "|"; // divides chunks from shards
		if (file.shards.get(sequence) != null && file.shards.get(sequence).size() != 0) {
			Vector<Shard> shards = file.shards.get(sequence);
			//System.out.println("DistributedFileCache getChunkStorageInfo: Shards vector length: " + shards.size());
			for (int i = 0; i < 9; i++) {
				boolean found = false;
				for (Shard shard : shards) {
					if (shard.shardnumber == i) {
						info += shard.serveridentifier + ",";
						found = true;
						break;
					}
				}
				if (!found) info += String.valueOf(-1) + ",";
			}
			info = info.substring(0,info.length()-1);
		}
		file = null;
		return info;
	}

	// Returns all the chunks removed so that new homes can be found for them.
	public synchronized Vector<ServerFile> removeAllFilesAtServer(int identifier) {
		Vector<ServerFile> totalRemoved = new Vector<ServerFile>(); // remove duplicates if there are any
		Vector<Chunk> removeChunks = new Vector<Chunk>();
		Vector<Shard> removeShards = new Vector<Shard>();
		Vector<String> removeFiles = new Vector<String>();
		Collection<DistributedFile> files = fileCache.values();
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
			if( file.shards != null && file.shards.size() != 0 ) {
				Collection<Vector<Shard>> shardVectors = file.shards.values();
				for (Vector<Shard> shardVector : shardVectors) {
					for (Shard shard : shardVector) {
						if (shard.serveridentifier == identifier) {
							removeShards.add(shard);
							totalRemoved.add(shard);
						}
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
		return fileCache.size();
	}

	public synchronized void addFile(String filename) {
		if (fileCache.containsKey(filename))
			return;
		DistributedFile newFile = new DistributedFile(filename);
		fileCache.put(filename,newFile);
	}

	public synchronized void removeFile(String filename) {
		if (!fileCache.containsKey(filename))
			return;
		fileCache.get(filename).clear();
		fileCache.remove(filename);
	}

	public synchronized void addChunk(Chunk chunk) {
		if (!fileCache.containsKey(chunk.filename)) // add file if not present
			this.addFile(chunk.filename);
		DistributedFile file = fileCache.get(chunk.filename);
		if (!file.chunks.containsKey(chunk.sequence))
			file.chunks.put(chunk.sequence,new Vector<Chunk>());
		file.chunks.get(chunk.sequence).addElement(chunk);
	}

	public synchronized void removeChunk(Chunk chunk) {
		if (!fileCache.containsKey(chunk.filename))
			return;
		DistributedFile file = fileCache.get(chunk.filename);
		if(!file.chunks.containsKey(chunk.sequence))
			return;
		file.chunks.get(chunk.sequence).removeElement(chunk); // remove chunk if it exists
	}

	public synchronized void addShard(Shard shard) {
		if (!fileCache.containsKey(shard.filename))
			this.addFile(shard.filename);
		DistributedFile file = fileCache.get(shard.filename);
		if (!file.shards.containsKey(shard.sequence)) {
			file.shards.put(shard.sequence,new Vector<Shard>());
		}
		file.shards.get(shard.sequence).addElement(shard);
	}

	public synchronized void removeShard(Shard shard) {
		if (!fileCache.containsKey(shard.filename))
			return;
		DistributedFile file = fileCache.get(shard.filename);
		if (!file.shards.containsKey(shard.sequence))
			return;
		file.shards.get(shard.sequence).removeElement(shard); // remove shard if it exists
	}

	public synchronized void clear() {
		Collection<DistributedFile> files = fileCache.values();
		for (DistributedFile file : files) {
			file.clear();
		}
		fileCache.clear();
	}

	public synchronized boolean containsChunk(Chunk chunk) {
		if (!fileCache.containsKey(chunk.filename) 
			|| !fileCache.get(chunk.filename).chunks.containsKey(chunk.sequence) 
			|| !fileCache.get(chunk.filename).chunks.get(chunk.sequence).contains(chunk)) {
			return false;
		}
		return true;
	}

	public synchronized boolean containsShard(Shard shard) {
		if (!fileCache.containsKey(shard.filename) 
			|| !fileCache.get(shard.filename).shards.containsKey(shard.sequence) 
			|| !fileCache.get(shard.filename).shards.get(shard.sequence).contains(shard)) {
			return false;
		}
		return true;
	}

	public synchronized void markChunkCorrupt(String filename, int sequence, int serveridentifier) {
		if (fileCache.containsKey(filename)
			&& fileCache.get(filename).chunks.containsKey(sequence)
			&& fileCache.get(filename).chunks.get(sequence) != null) {
			Vector<Chunk> vector = fileCache.get(filename).chunks.get(sequence);
			for (Chunk chunk : vector) {
				if (chunk.serveridentifier == serveridentifier)
					chunk.corrupt = true;
			}
		}
	}

	public synchronized void markShardCorrupt(String filename, int sequence, int shardnumber, int serveridentifier) {
		if (fileCache.containsKey(filename)
			&& fileCache.get(filename).shards.containsKey(sequence)
			&& fileCache.get(filename).shards.get(sequence) != null) {
			Vector<Shard> vector = fileCache.get(filename).shards.get(sequence);
			for (Shard shard : vector) {
				if (shard.serveridentifier == serveridentifier && shard.shardnumber == shardnumber)
					shard.corrupt = true;
			}
		}
	}

	public synchronized void markChunkHealthy(String filename, int sequence, int serveridentifier) {
		if (fileCache.containsKey(filename)
			&& fileCache.get(filename).chunks.containsKey(sequence)
			&& fileCache.get(filename).chunks.get(sequence) != null) {
			Vector<Chunk> vector = fileCache.get(filename).chunks.get(sequence);
			for (Chunk chunk : vector) {
				if (chunk.serveridentifier == serveridentifier)
					chunk.corrupt = false;
			}
		}
	}

	public synchronized void markShardHealthy(String filename, int sequence, int shardnumber, int serveridentifier) {
		if (fileCache.containsKey(filename)
			&& fileCache.get(filename).shards.containsKey(sequence)
			&& fileCache.get(filename).shards.get(sequence) != null) {
			Vector<Shard> vector = fileCache.get(filename).shards.get(sequence);
			for (Shard shard : vector) {
				if (shard.serveridentifier == serveridentifier && shard.shardnumber == shardnumber)
					shard.corrupt = false;
			}
		}
	}

	// Function to return differences between two DistributedFileCaches.
	// Will return all chunks or shards that are stored in this.fileCache
	// that are not stored in the the passed fileCache. Will help to 
	// identify chunkservers that are down.
	public synchronized Vector<String> differences(DistributedFileCache passedcache) {
		Vector<String> differences = new Vector<String>();
		for (Map.Entry<String,DistributedFile> entry1 : fileCache.entrySet()) {
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
						differences.add("chunk" + "," + filename + "," + sequence  + "," 
							+ chunk.version + "," + chunk.serveridentifier + "," + chunk.created);
					}
				}
			}
			for (Map.Entry<Integer,Vector<Shard>> entry2 : entry1.getValue().shards.entrySet()) {
				int sequence = entry2.getKey();
				for (Shard shard : entry2.getValue()) {
					if (!passedcache.containsShard(shard)) {
						differences.add("shard" + "," + filename + "," + sequence  + ","
							+ shard.shardnumber + "," + shard.serveridentifier + "," + shard.created);
					}
				}
			}
		}
		return differences;
	}
}