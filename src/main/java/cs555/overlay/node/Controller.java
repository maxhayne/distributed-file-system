package cs555.overlay.node;

import cs555.overlay.wireformats.*;
import cs555.overlay.transport.ChunkServerConnection;
import cs555.overlay.transport.ChunkServerConnectionCache;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.DistributedFileCache;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.Chunk;

import java.net.ServerSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;


public class Controller implements Node {

	private String host;
	private int port;
	private ChunkServerConnectionCache connectionCache;

	public Controller( String host, int port ) {
		this.host = host;
		this.port = port;
		this.connectionCache = new ChunkServerConnectionCache( 
			new DistributedFileCache(), new DistributedFileCache() );
	}

	public static void main(String[] args) throws Exception {
		
		try ( ServerSocket serverSocket = 
				new ServerSocket( ApplicationProperties.controllerPort ) ) {
				
			String host = serverSocket.getInetAddress().getHostAddress();
			Controller controller = new Controller( host, 
				ApplicationProperties.controllerPort );

			( new Thread ( new TCPServerThread( controller, 
				serverSocket ) ) ).start();

			System.out.println( "Controller's ServerThread has started at [" + host
				+ ":" + Integer.toString( ApplicationProperties.controllerPort )
				+ "]" );
			controller.interact();
		} catch ( IOException ioe ) {
			System.err.println( "Controller failed to start. " + ioe.getMessage());
			System.exit( 1 );
		}
		
	}
	
	public ChunkServerConnectionCache getConnectionCache() {
		return this.connectionCache;
	}

	@Override
	public String getHost() {
		return this.host;
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public void onEvent( Event event, TCPConnection connection ) {
		switch( event.getType() ) {
			
			case Protocol.CHUNK_SERVER_SENDS_REGISTRATION:
				registrationHelper( event, connection, true );
				break;

			case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION:
				registrationHelper( event, connection, false );
				break;

			case Protocol.CLIENT_REQUESTS_STORE_CHUNK:
				storeChunk( event, connection );
				break;
			
			case Protocol.CLIENT_REQUESTS_STORE_SHARDS:
				storeShards( event, connection );
				break;

			case Protocol.CLIENT_REQUESTS_FILE_DELETE:
				deleteFile( event, connection );
				break;

			case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT:
				heartbeatHelper( event );
				break;

			case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT:
				pokeHelper( event );
				break;
			
			case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION:
				corruptionHelper( event, connection );
				break;

			case Protocol.CHUNK_SERVER_NO_STORE_FILE:
				missingFileHelper( event );
				break;

			case Protocol.CHUNK_SERVER_REPORTS_FILE_FIX:
				markFileFixed( event );
				break;
			
			case Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO:
				clientRead( event, connection );
				break;
			
			case Protocol.CLIENT_REQUESTS_FILE_SIZE:
				fileSizeRequest( event, connection );
				break;
			
			case Protocol.CLIENT_REQUESTS_FILE_LIST:
				fileListRequest( event, connection );
				break;

			default:
				System.err.println( "Event couldn't be processed. "
					+ event.getType() );
				break;
		}
	}

	/**
	 * Respond to a request for the list of files stored on the DFS.
	 * @param event
	 * @param connection
	 */
	private void fileListRequest( Event event, TCPConnection connection ) {
		String filename = ( ( GeneralMessage ) event ).getMessage();

		ControllerSendsFileList response = 
			new ControllerSendsFileList( 
				connectionCache.getIdealState().getFileList() );
		
		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "fileListRequest: Unable to send resposne"
				+ " to Client containing list of files." );
		}
	} 

	/**
	 * Respond to a request for the file size of a particular file
	 * stored on the DFS (how many chunks it contains).
	 * @param event
	 * @param connection
	 */
	private void fileSizeRequest( Event event, TCPConnection connection ) {
		String filename = ( ( GeneralMessage ) event ).getMessage();
		ControllerReportsFileSize response = 
			new ControllerReportsFileSize( filename, 
				connectionCache.getIdealState().getFileSize( filename ) );
		
		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "fileSizeRequest: Unable to send response"
				+ " to Client with size of '" + filename + "'." );
		}
	}

	/**
	 * Gather information about where a particular file is stored on the DFS,
	 * whether that is a chunk, or an entire file, and send those storage
	 * details back to the Client.
	 * @param event
	 * @param connection
	 */
	private void clientRead( Event event, TCPConnection connection ) {
		GeneralMessage request = ( GeneralMessage ) event;
		
		String baseFilename;
		int sequence;
		// Check if the filename refers to a chunk, shard, or neither
		if ( FileDistributionService.checkChunkFilename( 
				request.getMessage() ) ) { // chunk
			String[] split = request.getMessage().split("_chunk");
			baseFilename = split[0];
			sequence = Integer.parseInt( split[1] );
		} else if ( FileDistributionService.checkShardFilename(
				request.getMessage() ) ) { // shard
			String[] split = request.getMessage().split("_chunk");
			baseFilename = split[0];
			split = split[1].split("_shard");
			sequence = Integer.parseInt( split[0] );
		} else {
			// Add in functionality for returning the storage information
			// about an entire file here...
			// For now, print an error message
			System.err.println( "clientRead: '" + request.getMessage()
				+ "' does not refer to a chunk or a shard." );
			return;
		}

		// Get storage information
		String storageInfo = connectionCache.getChunkStorageInfo( baseFilename, sequence );
		if ( storageInfo.equals("|") ) {
			System.err.println( "clientRead: '" + request.getMessage()
				+ "' is stored on no ChunkServer." );
			return; // There are no ChunkServers storing either chunks or shards
		}

		// Create response message
		String[] split = storageInfo.split("\\|", -1);
		String[] replications = split[0].split(",");
		String[] shards = split[1].split(",");
		ControllerSendsStorageList response = new ControllerSendsStorageList( baseFilename, 
			replications, shards );

		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "clientRead: Unable to send response to Client"
				+ " containing storage information about " + request.getMessage() + "." );
		}
	}

	/**
	 * Mark the chunk/shard healthy (not corrupt) in the reportedState 
	 * DistributedFileCache. This action will allow the Controller to tell
	 * future Clients or ChunkServers that this particular file at this
	 * particular server is available to be requested.
	 * @param event
	 */
	private void markFileFixed( Event event ) {
		ChunkServerReportsFileFix report = ( ChunkServerReportsFileFix ) event;

		// Mark the specified chunk/shard as healthy, so we can use this
		// ChunkServer as a source for future file requests.
		String baseFilename;
		int sequence;
		if ( FileDistributionService.checkChunkFilename( report.filename ) ) { // chunk
			String[] split = report.filename.split("_chunk");
			baseFilename = split[0];
			sequence = Integer.parseInt( split[1] );
			connectionCache.getReportedState().markChunkHealthy( baseFilename, sequence, 
				report.identifier ); // Mark healthy
		} else if ( FileDistributionService.checkShardFilename( report.filename ) ) { // shard
			String[] split = report.filename.split("_chunk");
			baseFilename  = split[0];
			split = split[1].split("_shard");
			sequence = Integer.parseInt( split[0] );
			int fragment = Integer.parseInt( split[1] );
			connectionCache.getReportedState().markShardHealthy( baseFilename, sequence, 
				fragment, report.identifier ); // Mark healthy
		} else {
			System.err.println( "markFileFixed: '" + report.filename + "' is not"
				+ " a valid name for either a chunk or a shard." );
			return;
		}
	}

	/**
	 * Update the idealState DistributedFileCache to indicate that the
	 * filename provided in the message is not stored there. Then, find
	 * a suitable ChunkServer to store the missing replication or shard.
	 * @param event
	 */
	private void missingFileHelper( Event event ) {
		ChunkServerNoStoreFile message = ( ChunkServerNoStoreFile ) event;
		// We need to remove the Chunk with those properties from the idealState,
		// and find a new server that can store the file.

		// Get address of ChunkServer that's missing the file
		String[] split = message.address.split(":");
		String host = split[0];
		int port = Integer.valueOf( split[1] );

		// Remove missing Chunk from idealState
		int identifier = connectionCache.getChunkServerIdentifier( host, port );
		int sequence = Integer.valueOf( message.filename.split("_chunk")[1] );
		if ( identifier != -1 ) {
			connectionCache.getIdealState().removeChunk( 
				new Chunk( message.filename, sequence, 0, identifier, false ) );
		}
		// Just remove the file for now. Can try to repair the file system during 
		// heartbeats for chunks that aren't replicated 3 times.
	}

	/**
	 * Changes the reportedState DistributedFileCache to reflect the fact
	 * that a file at the sender (a ChunkServer) is corrupt. Then, if possible,
	 * sends a response with a list of ChunkServers where replicas (or shards)
	 * for the same chunk are stored.
	 * @param event
	 */
	private void corruptionHelper( Event event, TCPConnection connection ) {
		ChunkServerReportsFileCorruption report = 
			( ChunkServerReportsFileCorruption ) event;
		
		// Mark the specified chunk/shard as corrupt, so that we don't tell
		// a Client to look there for a copy.
		String baseFilename;
		int sequence;
		if ( FileDistributionService.checkChunkFilename( report.filename ) ) {
			String[] split = report.filename.split("_chunk");
			baseFilename = split[0];
			sequence = Integer.parseInt( split[1] );
			connectionCache.getReportedState().markChunkCorrupt( baseFilename, sequence, 
				report.identifier ); // Mark the chunk corrupt
		} else if ( FileDistributionService.checkShardFilename( report.filename ) ) {
			String[] split = report.filename.split("_chunk");
			baseFilename = split[0];
			split = split[1].split("_shard");
			sequence = Integer.parseInt( split[0] );
			int fragment = Integer.parseInt( split[1] );
			connectionCache.getReportedState().markShardCorrupt( baseFilename, sequence, 
				fragment, report.identifier ); // Mark the shard corrupt
		} else {
			System.err.println( "corruptionHelper: '" + report.filename + "' is not"
				+ " a valid name for either a chunk or a shard." );
			return;
		}

		// Get list of ChunkServer host:port combos where chunk replacement
		// or shards are available.
		String info = connectionCache.getChunkStorageInfo( baseFilename, sequence );
		if ( info == null ) {
			System.err.println( "corruptionHelper: No servers could be found which" 
				+ " could help recreate '" + report.filename + "' on ChunkServer"
				+ report.identifier + "." );
			return;
		}

		// Send the storage information back to the ChunkServer with the
		// corrupt file.
		String[] split = info.split("\\|",-1);
		String[] replications = split[0].split(",");
		String[] shards = split[1].split(",");
		ControllerSendsStorageList response = 
			new ControllerSendsStorageList( report.filename, replications, shards );
		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "corruptionHelper: Unable to provide ChunkServer with"
				+ "information about how to replace its corrupt file." );
		}
	}

	/**
	 * Update ChunkServerConnection to reflect the fact that it responded
	 * to a poke from the Controller.
	 * @param event 
	 */
	private void pokeHelper( Event event ) {
		ChunkServerRespondsToHeartbeat response = 
			( ChunkServerRespondsToHeartbeat ) event;
		ChunkServerConnection connection = 
			connectionCache.getConnection( response.identifier );
		if ( connection == null ) {
			System.err.println( "pokeHelper: there is no registered ChunkServer"
				+ " with an identifier of " + response.identifier + "." );
			return;
		}
		connection.incrementPokeReplies();
	}

	/**
	 * Update ChunkServer's heartbeat information based on information
	 * sent in heartbeat message.
	 * @param event
	 */
	private void heartbeatHelper( Event event ) {
		ChunkServerSendsHeartbeat heartbeat = ( ChunkServerSendsHeartbeat ) event;
		// How should the updating of the heartbeat information actually
		// happen? In the same way as before, or does a new function need
		// to be written to update the heartbeat information all in one go?
		// It should be done with one function call.
		ChunkServerConnection connection = 
			connectionCache.getConnection( heartbeat.identifier );
		if ( connection == null ) {
			System.err.println( "heartbeatHelper: there is no registered ChunkServer"
				+ "with an identifier of " + heartbeat.identifier + "." );
			return;
		}
		connection.getHeartbeatInfo().update( heartbeat.type, heartbeat.freeSpace, 
			heartbeat.totalChunks, heartbeat.files );
	}

	/**
	 * Handles requests to delete a file at the Controller.
	 * @param event
	 * @param connection
	 */
	private void deleteFile( Event event, TCPConnection connection ) {
		GeneralMessage request = ( GeneralMessage ) event;

		// Remove file from DistributedFileCache
		connectionCache.getIdealState().removeFile( request.getMessage() );

		// Send delete request to all registered ChunkServers
		GeneralMessage deleteRequest = 
			new GeneralMessage( Protocol.CONTROLLER_REQUESTS_FILE_DELETE,
			request.getMessage() );
		try {
			connectionCache.sendToAll( deleteRequest.getBytes() );
		} catch( IOException ioe ) {
			System.err.println( "Error while sending file delete request "
				+ "to all ChunkServers. " + ioe.getMessage() );
		}

		// Send client an acknowledgement
		GeneralMessage response = new GeneralMessage( 
			Protocol.CONTROLLER_APPROVES_FILE_DELETE );
		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "Unable to acknowledge Client's request to " 
				+ "delete file. " + ioe.getMessage() );
		}
	}

	/**
	 * Handles requests to store a chunk at the Controller.
	 * @param event
	 * @param connection
	 */
	private void storeChunk( Event event, TCPConnection connection ) {
		ClientRequestsStoreChunk request = ( ClientRequestsStoreChunk ) event;

		// Try to add chunk and where it is stored to DistributedFileSystem
		String[] servers = connectionCache.availableChunkServers(
			request.filename, request.sequence ).split(",");
		
		// Choose which response to send
		Event response;
		if ( servers[0].equals( "" ) ) { // failure
			response = new GeneralMessage( Protocol.CONTROLLER_DENIES_STORAGE_REQUEST );
		} else { // success
			response = new ControllerSendsClientValidChunkServers( request.filename, 
				request.sequence, servers );
		}

		// Respond to Client
		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "Unable to respond to Client's request to store chunk. "
				+ ioe.getMessage() );
		}
	}

	/**
	 * Handles requests to store a chunk at the Controller.
	 * @param event
	 * @param connection
	 */
	private void storeShards( Event event, TCPConnection connection ) {
		ClientRequestsStoreShards request = ( ClientRequestsStoreShards ) event;

		// Try to add shard and where it is stored to DistributedFileCache
		String[] servers = connectionCache.availableShardServers(
			request.filename, request.sequence ).split(",");
		
		// Choose which response to send
		Event response;
		if ( servers[0].equals( "" ) ) { // failure
			response = new GeneralMessage( Protocol.CONTROLLER_DENIES_STORAGE_REQUEST );
		} else { // success
			response = new ControllerSendsClientValidShardServers( request.filename, 
				request.sequence, servers );
		}

		// Respond to Client
		try {
			connection.getSender().sendData( response.getBytes() );
		} catch ( IOException ioe ) {
			System.err.println( "Unable to respond to Client's request to store shards. "
				+ ioe.getMessage() );
		}
	}

	/**
	 * Handles registration requests at the Controller.
	 * @param event
	 * @param connection
	 * @param type true = register, false = deregister
	 */
	private void registrationHelper( Event event, TCPConnection connection, boolean type ) {
		if ( type ) { // attempt to register
			ChunkServerSendsRegistration request = ( ChunkServerSendsRegistration ) event;
			int registrationStatus = connectionCache.register( request.serverAddress, 
				request.serverPort, connection ); // attempt to register
			
			// Respond to ChunkServer
			ControllerReportsChunkServerRegistrationStatus response = new 
				ControllerReportsChunkServerRegistrationStatus( registrationStatus );
			try {
				connection.getSender().sendData( response.getBytes() );
			} catch ( IOException ioe ) {
				System.err.println( "Failed to notify ChunkServer of registration status. "
					+ "Deregistering. " + ioe.getMessage() );
				if ( registrationStatus != -1 ) {
					connectionCache.deregister( registrationStatus );
				}
			}
		} else { // deregister
			ChunkServerSendsDeregistration request = 
				( ChunkServerSendsDeregistration ) event;
			connectionCache.deregister( request.identifier );
		}
	}

	/**
	 * Loop here for user input to the Controller.
	 */
	private void interact() {
		System.out.println( "Enter a command or use 'help' to print a list of commands." );
        Scanner scanner = new Scanner( System.in );
        while ( true ) {
			String command = scanner.nextLine();
			String[] splitCommand = command.split("\\s+");
            switch ( splitCommand[0].toLowerCase() ) {
                case "help":
                    showHelp();
                    break;

                default:
                    System.err.println( "Unrecognized command. Use 'help' command." );
                    break;
            }
        }
	}

	private void showHelp() {
		System.out.printf( "%3s%-10s : %s%n", "", "help",
                "print a list of valid commands");
	}
}