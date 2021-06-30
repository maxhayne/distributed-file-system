package cs555.overlay.transport;
import cs555.overlay.transport.ChunkServerConnection;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.DistributedFileCache;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.util.Chunk;
import cs555.overlay.util.Shard;
import java.util.Collections;
import java.util.Collection;
import java.io.IOException;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Timer;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;

public class ChunkServerConnectionCache {

	private Map<Integer,ChunkServerConnection> chunkcache;
	private Vector<Integer> identifierList;
	private DistributedFileCache recommendations;
	private DistributedFileCache state;
	private HeartbeatMonitor heartbeatmonitor;
	private Timer heartbeattimer;

	public ChunkServerConnectionCache(DistributedFileCache recommendations, DistributedFileCache state) {
		this.chunkcache = new TreeMap<Integer,ChunkServerConnection>();
		this.identifierList = new Vector<Integer>();
		for (int i = 0; i < 32; i++)
			this.identifierList.add(i);
		Collections.shuffle(this.identifierList);
		this.recommendations = recommendations;
		this.state = state;
		this.heartbeatmonitor = new HeartbeatMonitor(this,chunkcache,recommendations,state);
		heartbeattimer = new Timer();
		heartbeattimer.scheduleAtFixedRate(heartbeatmonitor,0,30000L);
	}

	public String[] getFileList() {
		return recommendations.getFileList();
	}

	public int getFileSize(String filename) {
		return recommendations.getFileSize(filename);
	}

	public void removeFileFromRecommendations(String filename) {
		recommendations.removeFile(filename);
	}

	public void removeChunkFromRecommendations(Chunk chunk) {
		recommendations.removeChunk(chunk);
	}

	public String getAllServerAddresses() {
		String addresses = "";
		synchronized(chunkcache) {
			Collection<ChunkServerConnection> values = chunkcache.values();
			for (ChunkServerConnection connection : values) {
				addresses += connection.getServerAddress() + ":" + String.valueOf(connection.getServerPort()) + ",";
			}
			addresses = addresses.substring(0,addresses.length()-1);
		}
		return addresses;
	}

	public int getChunkServerIdentifier(String serveraddress, int serverport) {
		synchronized(chunkcache) {
			Collection<ChunkServerConnection> values = chunkcache.values();
			for (ChunkServerConnection connection : values) {
				if (connection.getServerAddress().equals(serveraddress) && connection.getServerPort() == serverport)
					return connection.getIdentifier();
			}
			return -1;
		}
	}

	public String getChunkServerServerAddress(int identifier) {
		synchronized(chunkcache) {
			if (chunkcache.get(identifier) != null)
				return chunkcache.get(identifier).getServerAddress() + ":" + chunkcache.get(identifier).getServerPort();
			return "";
		}
	}

	public String getChunkStorageInfo(String filename, int sequence) {
		String info;
		info = state.getChunkStorageInfo(filename,sequence);
		if (info.equals("|"))
			return "|";
		String[] parts = info.split("\\|",-1);
		String returnable = "";
		if (!parts[0].equals("")) {
			String[] replications = parts[0].split(",");
			boolean added = false;
			for (String replication : replications) {
				if (!getChunkServerServerAddress(Integer.parseInt(replication)).equals("")) {
					returnable += getChunkServerServerAddress(Integer.parseInt(replication))  + ",";
					added = true;
				}
			}
			if (added)
				returnable = returnable.substring(0,returnable.length()-1);
		}
		returnable += "|";
		if (!parts[1].equals("")) {
			String[] shardservers = parts[1].split(",");
			for (String shardserver : shardservers) {
				if (shardserver.equals("-1") || getChunkServerServerAddress(Integer.parseInt(shardserver)).equals(""))
					returnable += "-1,";
				else
					returnable += getChunkServerServerAddress(Integer.parseInt(shardserver)) + ",";
			}
			returnable = returnable.substring(0,returnable.length()-1);
		}
		return returnable;
	}

	// Return the best chunkservers in terms of storage
	public String availableChunkServers(String filename, int sequence) {
		// Need three freest servers
		synchronized(recommendations) { // If already allocated, return the same three servers
			if (!recommendations.getChunkStorageInfo(filename,sequence).split("\\|",-1)[0].equals("")) {
				String[] temp = recommendations.getChunkStorageInfo(filename,sequence).split("\\|",-1)[0].split(",");
				String servers = "";
				for (String server : temp) {
					servers += getChunkServerServerAddress(Integer.parseInt(server)) + ",";
				}
				servers = servers.substring(0,servers.length()-1);
				return servers;
			}
			Vector<Long[]> servers = new Vector<Long[]>();
			synchronized(chunkcache) {
				Collection<ChunkServerConnection> connections = chunkcache.values();
				for (ChunkServerConnection connection : connections) {
					if (connection.getUnhealthy() > 3 || connection.getFreeSpace() == -1 || connection.getFreeSpace() < 65720)
						continue;
					servers.add(new Long[]{(long)connection.getFreeSpace(),(long)connection.getIdentifier()});
				}
			}
			if (servers.size() < 3) return "";
			Collections.sort(servers, new Comparator<Long[]>(){
				@Override  
				public int compare(Long[] l1, Long[] l2) {
					int comparespace = l1[0].compareTo(l2[0]);
					if (comparespace == 0) {
						return l1[1].compareTo(l2[1]);
					}
					return comparespace;
			}});
			Collections.reverse(servers);
			String returnable = "";
			for (int i = 0; i < 3; i++) {
				Chunk chunk = new Chunk(filename,sequence,0,(int)(long)servers.elementAt(i)[1],false);
				recommendations.addChunk(chunk);
				returnable += String.valueOf(getChunkServerServerAddress((int)(long)servers.elementAt(i)[1])) + ",";
			}
			returnable = returnable.substring(0,returnable.length()-1);
			return returnable;
		}
	}

	// Return the best shardservers in terms of storage
	public String availableShardServers(String filename, int sequence) {
		// Need nine freest servers
		synchronized(recommendations) { // If already allocated, return the same three servers
			if (!recommendations.getChunkStorageInfo(filename,sequence).split("\\|",-1)[1].equals("")) {
				String[] temp = recommendations.getChunkStorageInfo(filename,sequence).split("\\|",-1)[1].split(",");
				String servers = "";
				for (String server : temp) {
					if (server.equals("-1")) {
						servers += "-1,";
						continue;
					}
					servers += getChunkServerServerAddress(Integer.parseInt(server)) + ",";
				}
				servers = servers.substring(0,servers.length()-1);
				return servers;
			}
			Vector<Long[]> servers = new Vector<Long[]>();
			synchronized(chunkcache) {
				Collection<ChunkServerConnection> connections = chunkcache.values();
				for (ChunkServerConnection connection : connections) {
					if (connection.getUnhealthy() > 3 || connection.getFreeSpace() == -1 || connection.getFreeSpace() < 65720)
						continue;
					servers.add(new Long[]{(long)connection.getFreeSpace(),(long)connection.getIdentifier()});
				}
			}
			if (servers.size() < 9) return "";
			Collections.sort(servers, new Comparator<Long[]>(){
				@Override  
				public int compare(Long[] l1, Long[] l2) {
					int comparespace = l1[0].compareTo(l2[0]);
					if (comparespace == 0) {
						return l1[1].compareTo(l2[1]);
					}
					return comparespace;
			}});
			Collections.reverse(servers);
			String returnable = "";
			for (int i = 0; i < 9; i++) {
				Shard shard = new Shard(filename,sequence,i,(int)(long)servers.elementAt(i)[1],false);
				recommendations.addShard(shard);
				returnable += returnable += String.valueOf(getChunkServerServerAddress((int)(long)servers.elementAt(i)[1])) + ",";
			}
			returnable = returnable.substring(0,returnable.length()-1);
			return returnable;
		}
	}

	public boolean isRegistered(String serveraddress, int serverport) {
		synchronized(chunkcache) {
			for (Map.Entry<Integer,ChunkServerConnection> entry : chunkcache.entrySet()) {
				if (serveraddress.equals(entry.getValue().getServerAddress())
					&& serverport == entry.getValue().getServerPort())
					return true;
			}
			return false;
		}
	}

	// Will return a valid ChunkServerConnection if successful, otherwise, a null pointer
	public ChunkServerConnection register(TCPReceiverThread tcpreceiverthread, String serveraddress, int serverport) throws IOException {
		// Check if already connected
		synchronized(chunkcache) {
			String ip = tcpreceiverthread.getRemoteAddress();
			int port = tcpreceiverthread.getRemotePort();
			for (Map.Entry<Integer,ChunkServerConnection> entry : chunkcache.entrySet()) {
				if (ip.equals(entry.getValue().getRemoteAddress())
					&& port == entry.getValue().getRemotePort())
					return null;
			}
		}
		// Check if there is an available ip
		int identifier;
		synchronized(identifierList) {
			if (identifierList.size() > 0) {
				identifier = identifierList.remove(identifierList.size()-1);
			} else {
				return null;
			}
		}
		// Create new ChunkServerConnection and add it to the chunkcache
		ChunkServerConnection newConnection = new ChunkServerConnection(tcpreceiverthread,identifier,serveraddress,serverport);
		synchronized(chunkcache) {
			chunkcache.put(identifier,newConnection);
			newConnection.start();
		}
		return newConnection;
	}

	// Must find new servers for its data before it goes offline
	public void deregister(int identifier) {
		System.out.println("Attempting to deregister if the logic is written.");
		// Need to get a list of all of the files that are stored at this node.
		// Can iterate through the recommendations filecache, or if the most recent heartbeat was 
		// a major one, could use that instead of locking down the filecache.
		// Once you get a list of files that were stored at the node, run through
		// the list, attempting to coax a ChunkServer to send it's replication to 
		// a different ChunkServer. If successful, remove the Chunk from the 
		// recommendations server and continue down the list.
		// During this process of trying to get ChunkServers to forward their
		// chunks or shards to new servers, files that are read could be discovered
		// to be corrupt, in which case they would need to be replaced, but that
		// should happen without the Controller node noticing. But, if a ChunkServer
		// doesn't respond with an acknowledgement, within a reasonable amount of time,
		// the next ChunkServer with the proper chunk replication should be contacted.

		// Also, the the deregister function can be called by the HeartbeatMonitor, if 
		// it notices that a node has been unresponsive for a measure of time, or if the
		// heartbeats don't line up with when they should be sent.

		// Also, shards that are stored at the server will not be able to be able to be
		// replaced, so don't even try to forward them anywhere else.

		// Remove it from the chunkcache
		synchronized(chunkcache) {
			chunkcache.get(identifier).setActiveStatus(false); // stop the thread
			chunkcache.remove(identifier);
		}
		synchronized(identifierList){
			identifierList.add(identifier);
		}
		Set<Chunk> removedStates = state.removeAllFilesAtServer(identifier);
		Set<Chunk> removedRecommendations = recommendations.removeAllFilesAtServer(identifier);
		// Care more about recommendations, want to find new homes for all the files
		// that were deleted at the server. Probably should be a FileHelperService, which takes
		// messages that should be sent to ChunkServers outside of the heartbeat message scheme.
		System.out.println("Removed from state:");
		for (Chunk chunk : removedStates) {
			System.out.println(chunk.print());
		}
		System.out.println("Removed from recommendations:");
		for (Chunk chunk : removedRecommendations) {
			System.out.println(chunk.print());
		}

		// Need to go through either removedStates or removedRecommendations to forward files
		// to new replication servers.

	}

	public void markChunkCorrupt(String filename, int sequence, int serveridentifier) {
		this.state.markChunkCorrupt(filename,sequence,serveridentifier);
	}

	public void markChunkHealthy(String filename, int sequence, int serveridentifier) {
		this.state.markChunkHealthy(filename,sequence,serveridentifier);
	}

	public void markShardCorrupt(String filename, int sequence, int shardnumber, int serveridentifier) {
		this.state.markShardCorrupt(filename,sequence,shardnumber,serveridentifier);
	}

	public void markShardHealthy(String filename, int sequence, int shardnumber, int serveridentifier) {
		this.state.markShardHealthy(filename,sequence,shardnumber,serveridentifier);
	}

	public void sendToChunkServer(int i, byte[] msg) {
		synchronized(chunkcache) {
			ChunkServerConnection connection = chunkcache.get(i);
			if (connection != null) {
				connection.addToSendQueue(msg);
			}
		}
	}
}