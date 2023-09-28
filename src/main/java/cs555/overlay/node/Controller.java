package cs555.overlay.node;

import cs555.overlay.wireformats.*;
import cs555.overlay.transport.ChunkServerConnectionCache;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.DistributedFileCache;

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

			default:
				System.err.println( "Event couldn't be processed. "
					+ event.getType() );
				break;
		}
	}

	/**
	 * Update ChunkServer's heartbeat information based on information
	 * sent in heartbeat message.
	 * @param event
	 */
	private void heartbeatHelper( Event event ) {
		ChunkServerSendsHeartbeat beat = ( ChunkServerSendsHeartbeat ) event;
		// How should the updating of the heartbeat information actually
		// happen? In the same way as before, or does a new function need
		// to be written to update the heartbeat information all in one go?
		// It should be done with one function call.
	}

	/**
	 * Handles requests to delete a file at the Controller.
	 * @param event
	 * @param connection
	 */
	private void deleteFile( Event event, TCPConnection connection ) {
		GeneralMessage request = ( GeneralMessage ) event;

		// Remove file from DistributedFileCache
		connectionCache.removeFileFromIdealState( request.getMessage() );

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