package cs555.overlay.util;
import cs555.overlay.wireformats.ControllerRequestsFileForward;
import cs555.overlay.wireformats.ControllerRequestsFileDelete;
import cs555.overlay.transport.ChunkServerConnectionCache;
import cs555.overlay.transport.ChunkServerConnection;
import cs555.overlay.transport.TCPSender;
import java.net.UnknownHostException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Vector;
import java.net.Socket;
import java.util.Map;

// Will be responsible for checking heartbeat data stored in the chunkcache
// Will set the 'healthy' boolean in every chunkconnection accordingly
// Will also cross-reference 'recommendations' with the actual state of what
// is stored across the chunkservers
public class HeartbeatMonitor extends TimerTask {

	private ChunkServerConnectionCache connectioncache;
	private Map<Integer,ChunkServerConnection> chunkcache;
	private DistributedFileCache recommendations;
	private DistributedFileCache state;

	public HeartbeatMonitor(ChunkServerConnectionCache connectioncache, Map<Integer,ChunkServerConnection> chunkcache, DistributedFileCache recommendations, DistributedFileCache state) {
		this.connectioncache = connectioncache;
		this.chunkcache = chunkcache;
		this.recommendations = recommendations;
		this.state = state;
	}

	private boolean checkChunkFilename(String filename) {
		boolean matches = filename.matches(".*_chunk[0-9]*$");
		String[] split = filename.split("_chunk");
		if (matches && split.length == 2) {
			return true;
		}
		return false;
	}

	private boolean checkShardFilename(String filename) {
		boolean matches = filename.matches(".*_chunk[0-9]*_shard[0-8]$");
		String[] split1 = filename.split("_chunk");
		String[] split2 = filename.split("_shard");
		if (matches && split1.length == 2 && split2.length == 2) {
			return true;
		}
		return false;
	}

	// If the version is -1, it is a shard, not a chunk!
	// Next step will be to determine if the the latest heartbeat is valid,
	// meaning it has the right type, and the timestamp is within 15 seconds of 
	// the heartbeat.

	public synchronized void run() {
		Vector<Integer> toRemove = new Vector<Integer>();
		synchronized(chunkcache) { // get lock on the chunkcache
			if (chunkcache == null || chunkcache.size() == 0)
				return;
			System.out.println("LISTING HEARTBEAT INFORMATION FOR ALL CHUNKSERVERS:");
			long now = System.currentTimeMillis();
			for (Map.Entry<Integer,ChunkServerConnection> entry : this.chunkcache.entrySet()) {
				ChunkServerConnection connection = entry.getValue();
				try {
					System.out.print(connection.print());
				} catch (UnknownHostException uhe) {
					System.err.println("This machine doesn't know its host address.");
				}
				byte[] temp = connection.retrieveHeartbeatInfo();
				System.out.println("HeartbeatInfo Length: " + temp.length);
				ByteBuffer data = ByteBuffer.wrap(temp);
				long lastMajorHeartbeat = data.getLong();
				long lastMinorHeartbeat = data.getLong();
				Vector<String> newfiles = new Vector<String>();
				int newchunks = data.getInt();
				System.out.println("Last Major Heartbeat: " + lastMajorHeartbeat);
				System.out.println("Last Minor Heartbeat: " + lastMinorHeartbeat);
				System.out.println("Total new files: " + newchunks);
				if (newchunks != 0) {
					System.out.print("New files: ");
					String newnames = "";
					for (int i = 0; i < newchunks; i++) {
						int namelength = data.getInt();
						byte[] array = new byte[namelength];
						data.get(array);
						String name = new String(array);
						newfiles.add(name);
						name = name.split(",")[0];
						newnames += name + ", ";
					}
					newnames = newnames.substring(0,newnames.length()-2);
					System.out.print(newnames + "\n");
				}

				long lastHeartbeat;
				int beattype;
				if (now-lastMajorHeartbeat < now-lastMinorHeartbeat) {
					lastHeartbeat = now-lastMajorHeartbeat;
					beattype = 1;
				} else {
					lastHeartbeat = now-lastMinorHeartbeat;
					beattype = 0;
				}

				// Do some logic on the state DistributedFileCache, assuming this is a healthy heartbeat
				if (lastHeartbeat < 30000L) {
					Vector<Chunk> newChunks = new Vector<Chunk>();
					Vector<Shard> newShards = new Vector<Shard>();
					for (String name : newfiles) {
						String[] split = name.split(",");
						int version = Integer.valueOf(split[1]);
						if (version == -1) { // shard
							if (!checkShardFilename(split[0]))
								continue;
							String[] chunkSplit = split[0].split("_chunk");
							// chunkSplit[0] is the filename
							String[] shardSplit = split[1].split("_shard");
							int sequence = Integer.valueOf(shardSplit[0]);
							int shardnumber = Integer.valueOf(shardSplit[1]);
							Shard newShard = new Shard(chunkSplit[0],sequence,shardnumber,connection.getIdentifier(),false);
							newShards.add(newShard);
						} else { // chunk
							if (!checkChunkFilename(split[0]))
								continue;
							String[] chunkSplit = split[0].split("_chunk");
							int sequence = Integer.valueOf(chunkSplit[1]);
							Chunk newChunk = new Chunk(chunkSplit[0],sequence,version,connection.getIdentifier(),false);
							newChunks.add(newChunk);
						}
					}
					if (beattype == 0) {
						// Add all the new files...
						for (Chunk chunk : newChunks)
							state.addChunk(chunk);
						for (Shard shard : newShards)
							state.addShard(shard);
						newChunks.clear();
						newShards.clear();
					} else {
						// Remove all of this node's old files from the filecache
						state.removeAllFilesAtServer(connection.getIdentifier());
						// and add all the new files...
						for (Chunk chunk : newChunks)
							state.addChunk(chunk);
						for (Shard shard : newShards)
							state.addShard(shard);
						newChunks.clear();
						newShards.clear();
					}
				}

				// Now need to handle some logic having to do with whether a chunkserver is alive
				int unhealthy = 0;
				if (lastMajorHeartbeat != -1 && now-lastMajorHeartbeat > 335000L)
					unhealthy += 1;
				if (lastMinorHeartbeat != -1 && now-lastMinorHeartbeat > 65000L)
					unhealthy += 1 + (int)((now-lastMinorHeartbeat-65000L)/30000); // extra unhealthy for more missed minor heartbeats
				if (now-connection.getStartTime() > 65000L && lastMinorHeartbeat == -1)
					unhealthy += 1;
				if (now-connection.getStartTime() > 335000L && lastMajorHeartbeat == -1)
					unhealthy += 1;

				// Can have total of 4 strikes the server, 2 or more should be considered unhealthy
				if (unhealthy >= 2)
					connection.incrementUnhealthy();
				else
					connection.decrementUnhealthy();

				// If this node has failed 3 times in a row, remove it from the cache of chunkservers
				if (connection.getUnhealthy() > 3) {
					toRemove.add(connection.getIdentifier());
				}
			}
			
			// Try to replace files that have been deleted from Chunk Servers, but should be there.
			try {
				// Can use this one to try to replace files that have somehow vanished from their correct nodes.
				System.out.println("Files in 'recommendations' that aren't in 'state':");
				Vector<String> diffs1 = recommendations.differences(state);
				for (String diff : diffs1) {
					// filetype,basename,sequence,version,serveridentifier
					// Use getChunkStorageInfo() in the chunkcache to get the list of addresses to send a forward request to
					System.out.println(diff);
					String[] parts = diff.split(",");
					if (parts[0].equals("shard")) continue; // Nothing we can do for shards
					String filename = parts[1] + "_chunk" + parts[2];
					String[] forwardServer = new String[1];
					forwardServer[0] = connectioncache.getChunkServerServerAddress(Integer.valueOf(parts[4]));
					if (forwardServer[0].equals("")) continue;
					String storageInfo = state.getChunkStorageInfo(parts[1],Integer.valueOf(parts[2]));
					String[] servers = storageInfo.split("\\|",-1);
					if (servers[0].equals("")) continue; // No server can forward
					String[] replications = servers[0].split(",");
					for (String server : replications) {
						if (server.equals(parts[0])) continue; // Don't send forward request to self.
						try {
							String address = connectioncache.getChunkServerServerAddress(Integer.valueOf(server));
							if (address.equals("")) continue;
							String hostname = address.split(":")[0];
							int port = Integer.valueOf(address.split(":")[1]);
							Socket connection = new Socket(hostname,port);
							TCPSender sender = new TCPSender(connection);
							ControllerRequestsFileForward forward = new ControllerRequestsFileForward(filename,forwardServer);
							sender.sendData(forward.getBytes());
							connection.close();
							sender = null;
						} catch (Exception e) {
							// This is best effort.
						}
					}
				}
			} catch (Exception e) {
				// All of this is best effort, so don't let anything derail the HeartbeatMonitor
			}

			// Try to delete files from Chunk Servers that shouldn't be there.
			try {
				// Can use this one to delete files that should no longer be where they are.
				System.out.println("Files in 'state' that aren't in 'recommendations':");
				Vector<String> diffs2 = state.differences(recommendations);
				for (String diff : diffs2) {
					// filetype,basename,sequence,shardnumber,serveridentifier
					System.out.println(diff);
					String[] parts = diff.split(",");
					ControllerRequestsFileDelete msg;
					if (parts[0].equals("shard")) {
						String filename = parts[1] + "_chunk" + parts[2] + "_shard" + parts[3];
						msg = new ControllerRequestsFileDelete(filename);
					} else {
						String filename = parts[1] + "_chunk" + parts[2];
						msg = new ControllerRequestsFileDelete(filename);
					}
					int identifier = Integer.valueOf(parts[4]);
					try {
						String address = connectioncache.getChunkServerServerAddress(identifier);
						if (address.equals("")) continue;
						String hostname = address.split(":")[0];
						int port = Integer.valueOf(address.split(":")[1]);
						Socket connection = new Socket(hostname,port);
						TCPSender sender = new TCPSender(connection);
						sender.sendData(msg.getBytes());
						connection.close();
						sender = null;
						// Remove the file from state
						if (parts[0].equals("shard")) {
							state.removeShard(new Shard(parts[1],Integer.valueOf(parts[2]),Integer.valueOf(parts[3]),Integer.valueOf(parts[4]),false));
						} else {
							state.removeChunk(new Chunk(parts[1],Integer.valueOf(parts[2]),Integer.valueOf(parts[3]),Integer.valueOf(parts[4]),false));
						}
					} catch (Exception e) {
						// This is best effort.
					}
				}
			} catch (Exception e) {
				// All of this is best effort, so don't let anything derail the HeartbeatMonitor
			}


			System.out.println();
		}
		for (Integer i : toRemove) {
			connectioncache.deregister(i);
		}
		toRemove.clear();
		toRemove = null;
	}
}