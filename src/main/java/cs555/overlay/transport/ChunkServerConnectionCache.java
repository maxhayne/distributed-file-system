package cs555.overlay.transport;
import cs555.overlay.wireformats.ControllerRequestsFileAcquire;
import cs555.overlay.transport.ChunkServerConnection;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.DistributedFileCache;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.transport.TCPSender;
import cs555.overlay.wireformats.Protocol;
import cs555.overlay.node.ChunkServer;
import cs555.overlay.util.ServerFile;
import cs555.overlay.util.Constants;
import cs555.overlay.util.Chunk;
import cs555.overlay.util.Shard;
import java.util.Collections;
import java.util.Collection;
import java.io.IOException;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Vector;
import java.util.Timer;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.net.Socket;

public class ChunkServerConnectionCache {

	private Map<Integer,ChunkServerConnection> chunkCache;
	private Vector<Integer> identifierList;
	private DistributedFileCache idealState;
	private DistributedFileCache reportedState;
	private HeartbeatMonitor heartbeatMonitor;
	private Timer heartbeatTimer;

	public ChunkServerConnectionCache(DistributedFileCache idealState, DistributedFileCache reportedState) {
		this.idealState = idealState;
		this.reportedState = reportedState;
		this.chunkCache = new TreeMap<Integer,ChunkServerConnection>();
		this.identifierList = new Vector<Integer>();
		for (int i = 0; i < 32; i++)
			this.identifierList.add(i);
		Collections.shuffle(this.identifierList);
		this.heartbeatMonitor = new HeartbeatMonitor(this,chunkCache,idealState,reportedState);
		heartbeatTimer = new Timer();
		heartbeatTimer.scheduleAtFixedRate(heartbeatMonitor,0,Constants.HEARTRATE);
	}

	public String[] getFileList() {
		return idealState.getFileList();
	}

	public int getFileSize(String filename) {
		return idealState.getFileSize(filename);
	}

	public void removeFileFromIdealState(String filename) {
		idealState.removeFile(filename);
	}

	public void removeChunkFromIdealState(Chunk chunk) {
		idealState.removeChunk(chunk);
	}

	public String getAllServerAddresses() {
		String addresses = "";
		synchronized(chunkCache) {
			Collection<ChunkServerConnection> values = chunkCache.values();
			for (ChunkServerConnection connection : values) {
				addresses += connection.getServerAddress() + ":" + String.valueOf(connection.getServerPort()) + ",";
			}
			if (!addresses.equals("")) addresses = addresses.substring(0,addresses.length()-1);
		}
		return addresses;
	}

	public int getChunkServerIdentifier(String serveraddress, int serverport) {
		synchronized(chunkCache) {
			Collection<ChunkServerConnection> values = chunkCache.values();
			for (ChunkServerConnection connection : values) {
				if (connection.getServerAddress().equals(serveraddress) && connection.getServerPort() == serverport)
					return connection.getIdentifier();
			}
			return -1;
		}
	}

	public String getChunkServerServerAddress(int identifier) {
		synchronized(chunkCache) {
			if (chunkCache.get(identifier) != null)
				return chunkCache.get(identifier).getServerAddress() + ":" + chunkCache.get(identifier).getServerPort();
			return "";
		}
	}

	public String getChunkStorageInfo(String filename, int sequence) {
		String info;
		info = reportedState.getChunkStorageInfo(filename,sequence);
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

	public String listFreestServers() {
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized(chunkCache) {
			Collection<ChunkServerConnection> connections = chunkCache.values();
			for (ChunkServerConnection connection : connections) {
				if (connection.getUnhealthy() > 3 || connection.getFreeSpace() == -1 || connection.getFreeSpace() < 65720)
					continue;
				servers.add(new Long[]{(long)connection.getFreeSpace(),(long)connection.getIdentifier()});
			}
		}
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
		for (int i = 0; i < servers.size(); i++) {
			returnable += String.valueOf(servers.elementAt(i)[1]) + ",";
		}
		if (!returnable.equals("")) returnable = returnable.substring(0,returnable.length()-1);
		return returnable;
	}

	// Return the best chunkservers in terms of storage
	public String availableChunkServers(String filename, int sequence) {
		// Need three freest servers
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized(chunkCache) {
			Collection<ChunkServerConnection> connections = chunkCache.values();
			for (ChunkServerConnection connection : connections) {
				if (connection.getUnhealthy() > 3 || connection.getFreeSpace() == -1 || connection.getFreeSpace() < 65720)
					continue;
				servers.add(new Long[]{(long)connection.getFreeSpace(),(long)connection.getIdentifier()});
			}
		}
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
		synchronized(idealState) { // If already allocated, return the same three servers
			if (!idealState.getChunkStorageInfo(filename,sequence).split("\\|",-1)[0].equals("")) {
				String[] temp = idealState.getChunkStorageInfo(filename,sequence).split("\\|",-1)[0].split(",");
				String allocatedservers = "";
				for (String server : temp) {
					allocatedservers += getChunkServerServerAddress(Integer.parseInt(server)) + ",";
				}
				allocatedservers = allocatedservers.substring(0,allocatedservers.length()-1);
				return allocatedservers;
			}
			if (servers.size() < 3) return "";
			String returnable = "";
			for (int i = 0; i < 3; i++) {
				Chunk chunk = new Chunk(filename,sequence,0,(int)(long)servers.elementAt(i)[1],false);
				idealState.addChunk(chunk);
				returnable += String.valueOf(getChunkServerServerAddress((int)(long)servers.elementAt(i)[1])) + ",";
			}
			returnable = returnable.substring(0,returnable.length()-1);
			return returnable;
		}
	}

	// Return the best shardservers in terms of storage
	public String availableShardServers(String filename, int sequence) {
		// Need nine freest servers
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized(chunkCache) {
			Collection<ChunkServerConnection> connections = chunkCache.values();
			for (ChunkServerConnection connection : connections) {
				if (connection.getUnhealthy() > 3 || connection.getFreeSpace() == -1 || connection.getFreeSpace() < 65720)
					continue;
				servers.add(new Long[]{(long)connection.getFreeSpace(),(long)connection.getIdentifier()});
			}
		}
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
		synchronized(idealState) { // If already allocated, return the same three servers
			if (!idealState.getChunkStorageInfo(filename,sequence).split("\\|",-1)[1].equals("")) {
				String[] temp = idealState.getChunkStorageInfo(filename,sequence).split("\\|",-1)[1].split(",");
				String allocatedservers = "";
				for (String server : temp) {
					if (server.equals("-1")) {
						allocatedservers += "-1,";
						continue;
					}
					allocatedservers += getChunkServerServerAddress(Integer.parseInt(server)) + ",";
				}
				allocatedservers = allocatedservers.substring(0,allocatedservers.length()-1);
				return allocatedservers;
			}
			if (servers.size() < 9) return "";
			String returnable = "";
			for (int i = 0; i < 9; i++) {
				Shard shard = new Shard(filename,sequence,i,(int)(long)servers.elementAt(i)[1],false);
				idealState.addShard(shard);
				//System.out.println(shard.print());
				returnable += getChunkServerServerAddress((int)(long)servers.elementAt(i)[1]) + ",";
			}
			returnable = returnable.substring(0,returnable.length()-1);
			return returnable;
		}
	}

	public boolean isRegistered(String serveraddress, int serverport) {
		synchronized(chunkCache) {
			for (Map.Entry<Integer,ChunkServerConnection> entry : chunkCache.entrySet()) {
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
		synchronized(chunkCache) {
			String ip = tcpreceiverthread.getRemoteAddress();
			int port = tcpreceiverthread.getRemotePort();
			for (Map.Entry<Integer,ChunkServerConnection> entry : chunkCache.entrySet()) {
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
		// Create new ChunkServerConnection and add it to the chunkCache
		ChunkServerConnection newConnection = new ChunkServerConnection(tcpreceiverthread,identifier,serveraddress,serverport);
		synchronized(chunkCache) {
			chunkCache.put(identifier,newConnection);
			newConnection.start();
		}
		return newConnection;
	}

	// Must find new servers for its data before it goes offline
	public void deregister(int identifier) {
		// Remove it from the chunkCache
		synchronized(chunkCache) {
			chunkCache.get(identifier).setActiveStatus(false); // stop sending thread
			chunkCache.get(identifier).close(); // stop the receiver 
			chunkCache.remove(identifier); // remove from the cache
		}
		synchronized(identifierList){
			identifierList.add(identifier);
		}
		Vector<ServerFile> removedReportedStates = reportedState.removeAllFilesAtServer(identifier);
		Vector<ServerFile> removedIdealStates = idealState.removeAllFilesAtServer(identifier);
		String list = listFreestServers();
		if (list.equals("")) return;
		String[] freestServers = list.split(",");
		for (ServerFile file : removedIdealStates) {
			String[] servers;
			if (file.getType().equals("CHUNK")) servers = idealState.getChunkStorageInfo(((Chunk)file).filename,((Chunk)file).sequence).split("\\|",-1)[0].split(",");
			else servers = idealState.getChunkStorageInfo(((Shard)file).filename,((Shard)file).sequence).split("\\|",-1)[1].split(",");
			if (servers[0].equals("")) continue;
			List<String> chunkservers = Arrays.asList(servers);
			for (String freeServer : freestServers) {
				if (chunkservers.contains(freeServer)) continue;
				int serveridentifier = Integer.valueOf(freeServer);
				ControllerRequestsFileAcquire acquire;
				if (file.getType().equals("CHUNK")) { // Chunk
					Chunk chunk = (Chunk)file;
					String fullFilename = chunk.filename + "_chunk" + String.valueOf(chunk.sequence);
					String[] serverAddresses = getChunkStorageInfo(chunk.filename,chunk.sequence).split("\\|",-1)[0].split(",");
					if (serverAddresses[0].equals("")) continue;
					acquire = new ControllerRequestsFileAcquire(fullFilename,serverAddresses);
					chunk.serveridentifier = serveridentifier;
					idealState.addChunk(chunk);
				} else { // Shard
					if (chunkservers.size() < Constants.DATA_SHARDS) break; // Don't bother if we can't rebuild
					Shard shard = (Shard)file;
					String fullFilename = shard.filename + "_chunk" + String.valueOf(shard.sequence) + "_shard" + String.valueOf(shard.shardnumber);
					String[] serverAddresses = getChunkStorageInfo(shard.filename,shard.sequence).split("\\|",-1)[1].split(",");
					if (serverAddresses[0].equals("")) continue;
					acquire = new ControllerRequestsFileAcquire(fullFilename,servers);
					shard.serveridentifier = serveridentifier;
					idealState.addShard(shard);
				}
				try { sendToChunkServer(serveridentifier,acquire.getBytes()); }
				catch (IOException ioe) {} // Best effort
				break;
			}
		}
	}

	public void markChunkCorrupt(String filename, int sequence, int serveridentifier) {
		this.reportedState.markChunkCorrupt(filename,sequence,serveridentifier);
	}

	public void markChunkHealthy(String filename, int sequence, int serveridentifier) {
		this.reportedState.markChunkHealthy(filename,sequence,serveridentifier);
	}

	public void markShardCorrupt(String filename, int sequence, int shardnumber, int serveridentifier) {
		this.reportedState.markShardCorrupt(filename,sequence,shardnumber,serveridentifier);
	}

	public void markShardHealthy(String filename, int sequence, int shardnumber, int serveridentifier) {
		this.reportedState.markShardHealthy(filename,sequence,shardnumber,serveridentifier);
	}

	public boolean sendToChunkServer(int i, byte[] msg) {
		synchronized(chunkCache) {
			ChunkServerConnection connection = chunkCache.get(i);
			if (connection != null) {
				connection.addToSendQueue(msg);
				return true;
			}
		}
		return false;
	}
}