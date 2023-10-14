package cs555.overlay.transport;

import cs555.overlay.wireformats.ControllerRequestsFileAcquire;
import cs555.overlay.util.DistributedFileCache;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.util.ServerFile;
import cs555.overlay.util.Constants;
import cs555.overlay.util.Chunk;
import cs555.overlay.util.Shard;

import java.util.Collections;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import java.util.Timer;
import java.util.Map;
import java.util.Comparator;
import java.util.List;
import java.util.Arrays;

public class ChunkServerConnectionCache {

	private final Vector<Integer> availableIdentifiers;
	private final Map<Integer,ChunkServerConnection> chunkCache;

	private final DistributedFileCache idealState;
	private final DistributedFileCache reportedState;

	private final HeartbeatMonitor heartbeatMonitor;
	private final Timer heartbeatTimer;

	public ChunkServerConnectionCache( DistributedFileCache idealState, 
			DistributedFileCache reportedState ) {
		this.idealState = idealState;
		this.reportedState = reportedState;

		this.chunkCache = new HashMap<Integer,ChunkServerConnection>();
		this.availableIdentifiers = new Vector<Integer>();
		for ( int i = 1; i <= 32; ++i ) {
			this.availableIdentifiers.add( i );
		}

		this.heartbeatMonitor = new HeartbeatMonitor (this, chunkCache, idealState, 
			reportedState );

		this.heartbeatTimer = new Timer();
		this.heartbeatTimer.scheduleAtFixedRate( heartbeatMonitor, 0,
			Constants.HEARTRATE );
	}

	/**
	 * Returns the ChunkServerConnection object of a registered ChunkServer
	 * with the identifier specified as a parameter.
	 * 
	 * @param identifier of ChunkServer
	 * @return ChunkServerConnection with that identifier, null if doesn't exist
	 */
	public ChunkServerConnection getConnection( int identifier ) {
		synchronized( chunkCache ) {
			return chunkCache.get( identifier );
		}
	}

	public DistributedFileCache getIdealState() {
		return idealState;
	}

	public DistributedFileCache getReportedState() {
		return reportedState;
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

	public int getChunkServerIdentifier( String serveraddress, int serverport ) {
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if ( connection.getServerAddress().equals( serveraddress ) 
						&& connection.getServerPort() == serverport ) {
					return connection.getIdentifier();
				}
			}
		}
		return -1;
	}

	public String getChunkServerServerAddress( int identifier ) {
		synchronized( chunkCache ) {
			ChunkServerConnection connection = chunkCache.get( identifier );
			if ( connection != null ) {
				return connection.getServerAddress() + ":" 
					+ connection.getServerPort();
			}
		}
		return "";
	}

	public String getChunkStorageInfo( String filename, int sequence ) {
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
				returnable = returnable.substring( 0, returnable.length()-1 ) ;
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
				if (connection.getUnhealthy() <= 3 && connection.getHeartbeatInfo().getFreeSpace() != -1 
						&& connection.getHeartbeatInfo().getFreeSpace() >= 65720) {
					servers.add( new Long[]{ connection.getHeartbeatInfo().getFreeSpace(),
						(long)connection.getIdentifier() } );
				}
			}
		}

		Collections.sort( servers, Comparators.SERVER_SORT );
		Collections.reverse( servers );

		Vector<String> freestServers = new Vector<String>();
		for (int i = 0; i < servers.size(); i++) {
			freestServers.add( String.valueOf( servers.elementAt(i)[1] ) );
		}
		return freestServers;
	}

	// Return the best ChunkServers in terms of storage
	public String availableChunkServers( String filename, int sequence ) {
		// Need three freest servers
		Vector<Long[]> servers = new Vector<Long[]>();
		synchronized( chunkCache ) {
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if ( connection.getUnhealthy() <= 3 && connection.getHeartbeatInfo().getFreeSpace() != -1 
						&& connection.getHeartbeatInfo().getFreeSpace() >= 65720 ) {
					servers.add( new Long[]{ connection.getHeartbeatInfo().getFreeSpace(),
						(long)connection.getIdentifier() } );
				}
			}
		}

		Collections.sort( servers, Comparators.SERVER_SORT );
		Collections.reverse( servers );

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
				Chunk chunk = new Chunk( filename, sequence, 0, 0,
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
				if ( connection.getUnhealthy() <= 3 && connection.getHeartbeatInfo().getFreeSpace() != -1 
						&& connection.getHeartbeatInfo().getFreeSpace() >= 65720 ) {
					servers.add( new Long[]{ connection.getHeartbeatInfo().getFreeSpace(),
						(long)connection.getIdentifier() } );
				}
			}
		}

		Collections.sort( servers, Comparators.SERVER_SORT );
		Collections.reverse( servers );
		
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
				Shard shard = new Shard(filename,sequence,i,0, 0,
					(int)(long)servers.elementAt(i)[1],false);
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
			for ( ChunkServerConnection connection : chunkCache.values() ) {
				if ( host.equals( connection.getServerAddress() )
						&& port == connection.getServerPort()) {
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
				if ( !availableIdentifiers.isEmpty() && !isRegistered( host, port ) ) {
					int identifier = availableIdentifiers.remove( availableIdentifiers.size()-1 );
					ChunkServerConnection newConnection = new ChunkServerConnection( 
						identifier, host, port, connection );
					( new Thread( newConnection ) ).start(); // start run loop
					chunkCache.put( identifier, newConnection );
					registrationStatus = identifier; // registration successful
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
				connection.getConnection().close(); // stop the receiver
				chunkCache.remove( identifier );
				availableIdentifiers.add( identifier ); // add back identifier
				
				// Must be sure that this is a safe operation
				removedIdealStates = idealState.removeAllFilesAtServer( identifier );
				reportedState.removeAllFilesAtServer( identifier );
			}
		}

		// Get best candidates for relocation
		Vector<String> freestServers = listFreestServers();
		if ( freestServers.isEmpty() ) { // no servers left for relocation
			return;
		}

		// Iterate through displaced replicas, relocated to freest ChunkServers
		for ( ServerFile file : removedIdealStates ) {
			String[] servers;
			if ( file.getType() == Constants.CHUNK_TYPE ) {
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
				int serverIdentifier = Integer.parseInt(freeServer);
				ControllerRequestsFileAcquire acquire;
				if ( file.getType() == Constants.CHUNK_TYPE ) { // Chunk
					Chunk chunk = (Chunk)file;
					String fullFilename = chunk.filename + "_chunk" + String.valueOf(chunk.sequence);
					String[] serverAddresses = getChunkStorageInfo(chunk.filename, chunk.sequence)
						.split("\\|",-1)[0].split(",");
					if ( serverAddresses[0].equals("") ) {
						continue;
					}
					acquire = new ControllerRequestsFileAcquire( fullFilename, serverAddresses );
					chunk.serverIdentifier = serverIdentifier;
					idealState.addChunk(chunk);
				} else { // Shard
					if (chunkServers.size() < Constants.DATA_SHARDS) break; // Don't bother if we can't rebuild
					Shard shard = (Shard)file;
					String fullFilename = shard.filename + "_chunk" + String.valueOf(shard.sequence)
						+ "_shard" + String.valueOf(shard.fragment);
					String[] serverAddresses = getChunkStorageInfo(shard.filename, shard.sequence)
						.split("\\|",-1)[1].split(",");
					if (serverAddresses[0].equals("")) {
						continue;
					}
					acquire = new ControllerRequestsFileAcquire(fullFilename, servers);
					shard.serverIdentifier = serverIdentifier;
					idealState.addShard(shard);
				}
				try {
					sendToChunkServer( serverIdentifier,acquire.getBytes() );
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
	public boolean sendToChunkServer( int identifier, byte[] marshalledBytes ) {
		synchronized( chunkCache ) {
			ChunkServerConnection connection = chunkCache.get( identifier );
			if ( connection != null ) {
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

	/**
	 * Class to house Comparators which will be used in the methods of
	 * the ChunkServerConnectionCache.
	 */
	public static class Comparators {

		// Will be provided a Long[] filled with Long[] tuples. Each tuple
		// is formatted [ FREE_SPACE, SERVER_ID ]. We want to sort based on
		// FREE_SPACE first, then by ID.
		public static Comparator<Long[]> SERVER_SORT = new Comparator<Long[]>() {
			@Override  
			public int compare(Long[] l1, Long[] l2) {
				int compareSpace = l1[0].compareTo( l2[0] );
				if (compareSpace == 0) {
					return l1[1].compareTo( l2[1] );
				}
				return compareSpace;
			}
		};

    }
}