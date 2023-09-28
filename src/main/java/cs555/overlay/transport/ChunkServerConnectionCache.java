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

import java.util.concurrent.BlockingQueue;
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

	//private BlockingQueue<Integer> availableIdentifiers;
	private Vector<Integer> availableIdentifiers;
	private Map<Integer,ChunkServerConnection> chunkCache;

	private DistributedFileCache idealState;
	private DistributedFileCache reportedState;

	private HeartbeatMonitor heartbeatMonitor;
	private Timer heartbeatTimer;

	public ChunkServerConnectionCache( DistributedFileCache idealState, 
			DistributedFileCache reportedState ) {
		this.idealState = idealState;
		this.reportedState = reportedState;

		this.chunkCache = new TreeMap<Integer,ChunkServerConnection>();
		this.availableIdentifiers = new Vector<Integer>();
		for ( int i = 0; i < 32; ++i ) {
			this.availableIdentifiers.add( i );
		}
		//Collections.shuffle(this.availableIdentifiers);

		this.heartbeatMonitor = new HeartbeatMonitor (this, chunkCache,idealState, 
			reportedState );
		heartbeatTimer = new Timer();
		heartbeatTimer.scheduleAtFixedRate( heartbeatMonitor, 0, 
			Constants.HEARTRATE);
	}

	public DistributedFileCache getIdealState() {
		return this.idealState;
	}

	public DistributedFileCache getReportedState() {
		return this.reportedState;
	}

	public String[] getFileList() {
		return idealState.getFileList();
	}

	public int getFileSize( String filename ) {
		return idealState.getFileSize( filename );
	}

	public void removeFileFromIdealState( String filename ) {
		idealState.removeFile(filename );
	}

	public void removeChunkFromIdealState( Chunk chunk ) {
		idealState.removeChunk( chunk );
	}

	public void markChunkCorrupt(String filename, int sequence, int serverIdentifier) {
		this.reportedState.markChunkCorrupt(filename,sequence,serverIdentifier);
	}

	public void markChunkHealthy(String filename, int sequence, int serverIdentifier) {
		this.reportedState.markChunkHealthy(filename,sequence,serverIdentifier);
	}

	public void markShardCorrupt(String filename, int sequence, int shardnumber, int serverIdentifier) {
		this.reportedState.markShardCorrupt(filename,sequence,shardnumber,serverIdentifier);
	}

	public void markShardHealthy(String filename, int sequence, int shardnumber, int serverIdentifier) {
		this.reportedState.markShardHealthy(filename,sequence,shardnumber,serverIdentifier);
	}

	public String getAllServerAddresses() {
		String addresses = "";
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				addresses += connection.getServerAddress() + ":" 
					+ String.valueOf( connection.getServerPort() ) + ",";
			}
			if ( !addresses.equals( "" ) ) {
				addresses = addresses.substring( 0, addresses.length()-1 );
			}
		}
		return addresses;
	}

	public int getChunkServerIdentifier(String serveraddress, int serverport) {
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if ( connection.getServerAddress().equals(serveraddress) 
						&& connection.getServerPort() == serverport ) {
					return connection.getIdentifier();
				}
			}
			return -1;
		}
	}

	public String getChunkServerServerAddress( int identifier ) {
		synchronized( chunkCache ) {
			ChunkServerConnection connection = chunkCache.get( identifier );
			if ( connection != null ) {
				return connection.getServerAddress() + ":" 
					+ connection.getServerPort();
			}
			return "";
		}
	}

	public String getChunkStorageInfo(String filename, int sequence) {
		String info = reportedState.getChunkStorageInfo( filename, sequence );
		if ( info.equals("|") ) {
			return "|";
		}
		String[] parts = info.split("\\|", -1);
		String returnable = "";
		if ( !parts[0].equals("") ) {
			String[] replications = parts[0].split(",");
			boolean added = false;
			for ( String replication : replications ) {
				if ( !getChunkServerServerAddress( Integer.parseInt(replication) )
						.equals("") ) {
					returnable += getChunkServerServerAddress( Integer.parseInt( replication ) )
						+ ",";
					added = true;
				}
			}
			if (added) {
				returnable = returnable.substring( 0, returnable.length()-1) ;
			}
		}
		returnable += "|";
		if ( !parts[1].equals("") ) {
			String[] shardservers = parts[1].split(",");
			for ( String shardserver : shardservers ) {
				if ( shardserver.equals("-1") 
						|| getChunkServerServerAddress( Integer.parseInt( shardserver ))
							.equals("") ) {
					returnable += "-1,";
				} else {
					returnable += getChunkServerServerAddress( Integer.parseInt( shardserver ) )
						+ ",";
				}
			}
			returnable = returnable.substring( 0, returnable.length()-1 );
		}
		return returnable;
	}

	public Vector<String> listFreestServers() {
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if (connection.getUnhealthy() <= 3 && connection.getFreeSpace() != -1 
						&& connection.getFreeSpace() >= 65720) {
					servers.add( new Long[]{(long)connection.getFreeSpace(),
						(long)connection.getIdentifier()} );
				}
			}
		}

		Collections.sort(servers, new Comparator<Long[]>(){
			@Override  
			public int compare(Long[] l1, Long[] l2) {
				int compareSpace = l1[0].compareTo(l2[0]);
				if ( compareSpace == 0 ) {
					return l1[1].compareTo(l2[1]);
				}
				return compareSpace;
		}});

		Collections.reverse( servers );
		Vector<String> freestServers = new Vector<String>();
		//String returnable = "";
		for (int i = 0; i < servers.size(); i++) {
			//returnable += String.valueOf(servers.elementAt(i)[1]) + ",";
			freestServers.add( String.valueOf( servers.elementAt(i)[1] ) );
		}
		//if (!returnable.equals("")) returnable = returnable.substring(0,returnable.length()-1);
		return freestServers;
	}

	// Return the best ChunkServers in terms of storage
	public String availableChunkServers(String filename, int sequence) {
		// Need three freest servers
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if ( connection.getUnhealthy() <= 3 && connection.getFreeSpace() != -1 
						&& connection.getFreeSpace() >= 65720 ) {
					servers.add( new Long[]{(long)connection.getFreeSpace(),
						(long)connection.getIdentifier()} );
				}
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
		synchronized( idealState ) { // If already allocated, return the same three servers
			if ( !idealState.getChunkStorageInfo(filename, sequence)
					.split("\\|", -1)[0].equals("") ) {
				String[] temp = idealState.getChunkStorageInfo(filename, sequence)
					.split("\\|", -1)[0].split(",");
				String allocatedservers = "";
				for (String server : temp) {
					allocatedservers += getChunkServerServerAddress( Integer.parseInt( server ) )
					+ ",";
				}
				allocatedservers = allocatedservers.substring( 0, allocatedservers.length()-1 );
				return allocatedservers;
			}
			if ( servers.size() < 3 ) {
				return "";
			}
			String returnable = "";
			for ( int i = 0; i < 3; i++ ) {
				Chunk chunk = new Chunk( filename, sequence, 0, 
					(int)(long)servers.elementAt(i)[1], false);
				idealState.addChunk( chunk );
				returnable += String.valueOf( getChunkServerServerAddress((int)(long)servers
					.elementAt(i)[1]) ) + ",";
			}
			returnable = returnable.substring( 0, returnable.length()-1 );
			return returnable;
		}
	}

	// Return the best shardservers in terms of storage
	public String availableShardServers( String filename, int sequence ) {
		// Need nine freest servers
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if ( connection.getUnhealthy() <= 3 && connection.getFreeSpace() != -1 
						&& connection.getFreeSpace() >= 65720 ) {
					servers.add( new Long[]{(long)connection.getFreeSpace(),
						(long)connection.getIdentifier()} );
				}
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
		synchronized( idealState ) { // If already allocated, return the same three servers
			if ( !idealState.getChunkStorageInfo(filename, sequence).split("\\|",-1)[1].equals("") ) {
				String[] temp = idealState.getChunkStorageInfo(filename, sequence)
					.split("\\|",-1)[1].split(",");
				String allocatedservers = "";
				for ( String server : temp ) {
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

	/**
	 * Returns whether particular host:port combination has
	 * registered as a ChunkServer.
	 * @param host
	 * @param port
	 * @return true if registered, false if not
	 */
	public boolean isRegistered( String host, int port ) {
		synchronized( chunkCache ) {
			for ( Map.Entry<Integer,ChunkServerConnection> entry : chunkCache.entrySet() ) {
				if ( host.equals( entry.getValue().getServerAddress() )
					&& port == entry.getValue().getServerPort()) {
					return true;
				}
			}
			return false;
		}
	}

	/*
	// Old register method
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
		// Check if there is an available id
		int identifier;
		synchronized( availableIdentifiers ) {
			if (availableIdentifiers.size() > 0) {
				identifier = availableIdentifiers.remove(availableIdentifiers.size()-1);
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
	*/

	/**
	 * Attempts to register the host:port combination as a ChunkServer.
	 * If registration fails, returns -1, else returns identifier.
	 * @return status of registration attempt
	 */
	public int register( String host, int port, TCPConnection connection ) {
		int registrationStatus = -1; // -1 is a failure
		synchronized( chunkCache ) {
			synchronized( availableIdentifiers ) {
				if ( availableIdentifiers.size() > 0 && !isRegistered( host, port ) ) {
					int identifier = availableIdentifiers.remove( availableIdentifiers.size()-1 );
					ChunkServerConnection newConnection = new ChunkServerConnection( 
						identifier, host, port, connection );
					newConnection.start(); // start the run loop
					chunkCache.put( identifier, newConnection );
					registrationStatus = identifier; // registration successful
				} else {
					return registrationStatus; // registration unsuccessful
				}
			}
		}
		return registrationStatus;
	}

	/**
	 * Removes the ChunkServer with a particular identifier from the
	 * ChunkServerConnectionCache. Since this ChunkServer may be storing essential
	 * files for the operation of the distributed file system, files stored on
	 * the ChunkServer must be relocated to other available ChunkServers.
	 * @param identifier
	 */
	public void deregister( int identifier ) {
		// Remove from the chunkCache and availableIdentifiers
		// Remove all instances of identifier from each DistributedFileCache
		Vector<ServerFile> removedIdealStates;
		synchronized(chunkCache) {
			synchronized( availableIdentifiers ) {
				ChunkServerConnection connection = chunkCache.get( identifier );
				if ( connection == null ) { // no ChunkServer to remove
					return;
				}
				connection.setActiveStatus( false ); // stop sending thread
				connection.close(); // stop the receiver 
				chunkCache.remove( identifier );
				availableIdentifiers.add( identifier ); // add back identifier
				
				// Must be sure that this is a safe operation
				removedIdealStates = idealState.removeAllFilesAtServer( identifier );
				reportedState.removeAllFilesAtServer( identifier );
			}
		}

		// Get best candidates for relocation
		Vector<String> freestServers = listFreestServers();
		if ( freestServers.size() == 0 ) { // no servers left for relocation
			return;
		}

		// Iterate through displaced replicas, relocated to freest ChunkServers
		for ( ServerFile file : removedIdealStates ) {
			String[] servers;
			if ( file.getType().equals("CHUNK") ) {
				servers = idealState.getChunkStorageInfo( ((Chunk)file).filename, 
					((Chunk)file).sequence ).split("\\|",-1)[0].split(",");
			} else {
				servers = idealState.getChunkStorageInfo( ((Shard)file).filename,
					((Shard)file).sequence ).split("\\|",-1)[1].split(",");
			}
			if ( servers[0].equals( "" ) ) {
				continue;
			}
			List<String> chunkServers = Arrays.asList( servers );
			for ( String freeServer : freestServers ) {
				if ( chunkServers.contains(freeServer) ) {
					continue; // don't store two replicas on one ChunkServer
				}
				int serverIdentifier = Integer.valueOf(freeServer);
				ControllerRequestsFileAcquire acquire;
				if (file.getType().equals("CHUNK")) { // Chunk
					Chunk chunk = (Chunk)file;
					String fullFilename = chunk.filename + "_chunk" + String.valueOf(chunk.sequence);
					String[] serverAddresses = getChunkStorageInfo(chunk.filename,chunk.sequence)
						.split("\\|",-1)[0].split(",");
					if ( serverAddresses[0].equals("") ) {
						continue;
					}
					acquire = new ControllerRequestsFileAcquire( fullFilename, serverAddresses );
					chunk.serveridentifier = serverIdentifier;
					idealState.addChunk(chunk);
				} else { // Shard
					if (chunkServers.size() < Constants.DATA_SHARDS) break; // Don't bother if we can't rebuild
					Shard shard = (Shard)file;
					String fullFilename = shard.filename + "_chunk" + String.valueOf(shard.sequence)
						+ "_shard" + String.valueOf(shard.shardnumber);
					String[] serverAddresses = getChunkStorageInfo(shard.filename,shard.sequence)
						.split("\\|",-1)[1].split(",");
					if (serverAddresses[0].equals("")) {
						continue;
					}
					acquire = new ControllerRequestsFileAcquire(fullFilename,servers);
					shard.serveridentifier = serverIdentifier;
					idealState.addShard(shard);
				}
				try { sendToChunkServer( 
					serverIdentifier,acquire.getBytes() );
				} catch (IOException ioe) {} // Best effort
				break;
			}
		}
	}

	/**
	 * Add message to send queue for a specific ChunkServer.
	 * @param identifier
	 * @param marshalledBytes
	 * @return true if message was successfully added to the queue to send, false otherwise
	 */
	public boolean sendToChunkServer(int identifier, byte[] marshalledBytes ) {
		synchronized( chunkCache ) {
			ChunkServerConnection connection = chunkCache.get( identifier );
			if (connection != null) {
				connection.addToSendQueue( marshalledBytes );
				return true;
			}
		}
		return false;
	}

	/**
	 * Add message to send queue for all registered ChunkServers.
	 * Useful for file deletion requests.
	 * @param marshalledBytes
	 */
	public void sendToAll( byte[] marshalledBytes ) {
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				connection.addToSendQueue( marshalledBytes );
			}
		}
	} 
}