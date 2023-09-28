package cs555.overlay.wireformats;

import java.io.IOException;

/**
 * Factory to create the type of event which is received
 * in a TCPReceiverThread. Implemented as singleton. To,
 * in theory, save space.
 * 
 * @author hayne
 */
public class EventFactory {

	private static final EventFactory eventFactory = new EventFactory();

    /**
     * Private Constructor.
     */
    private EventFactory() {}

    /**
     * Gets instance of singleton EventFactory.
     *
     * @return eventFactory singleton
     */
    public static EventFactory getInstance() { return eventFactory; }

	/**
	 * Create new event by inspecting the message type in
	 * the first location of the byte message.
	 * @param marshalledBytes
	 * @return event created by unmarshalling bytes
	 * 
	 */
	public Event createEvent( byte[] marshalledBytes ) throws IOException {
		switch( marshalledBytes[0] ) {
			case Protocol.CONTROLLER_DENIES_STORAGE_REQUEST:
			case Protocol.CONTROLLER_SENDS_HEARTBEAT:
			case Protocol.CLIENT_REQUESTS_FILE_LIST:
			case Protocol.CONTROLLER_APPROVES_FILE_DELETE:
				return new GeneralMessage( marshalledBytes );
			
			case Protocol.CHUNK_SERVER_SENDS_REGISTRATION:
				return new ChunkServerSendsRegistration( marshalledBytes );

			case Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS:
				return new ControllerReportsChunkServerRegistrationStatus( marshalledBytes );

			case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION:
				return new ChunkServerSendsDeregistration( marshalledBytes );

			case Protocol.CLIENT_REQUESTS_STORE_CHUNK:
				return new ClientRequestsStoreChunk( marshalledBytes );

			case Protocol.CONTROLLER_SENDS_CLIENT_VALID_CHUNK_SERVERS:
				return new ControllerSendsClientValidChunkServers( marshalledBytes );

			case Protocol.CLIENT_REQUESTS_STORE_SHARDS:
				return new ClientRequestsStoreShards( marshalledBytes );

			case Protocol.CONTROLLER_SENDS_CLIENT_VALID_SHARD_SERVERS:
				return new ControllerSendsClientValidShardServers( marshalledBytes );

			case Protocol.CLIENT_REQUESTS_FILE_DELETE:
				return new ClientRequestsFileDelete( marshalledBytes );

			case Protocol.CONTROLLER_REQUESTS_FILE_DELETE:
				return new ControllerRequestsFileDelete( marshalledBytes );

			case Protocol.SENDS_FILE_FOR_STORAGE:
				return new SendsFileForStorage( marshalledBytes );

			case Protocol.REQUESTS_CHUNK:
				return new RequestsChunk( marshalledBytes );

			case Protocol.REQUESTS_SHARD:
				return new RequestsShard( marshalledBytes );

			case Protocol.CHUNK_SERVER_DENIES_REQUEST:
				return new ChunkServerDeniesRequest( marshalledBytes );

			case Protocol.CHUNK_SERVER_SERVES_FILE:
				return new ChunkServerServesFile( marshalledBytes );

			case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT:
				return new ChunkServerSendsHeartbeat( marshalledBytes );

			case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT:
				return new ChunkServerRespondsToHeartbeat();

			case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION:
				return new ChunkServerReportsFileCorruption( marshalledBytes );

			case Protocol.CONTROLLER_REQUESTS_FILE_ACQUIRE:
				return new ControllerRequestsFileAcquire( marshalledBytes );

			default:
				System.err.println( "Event couldn't be created. "
					+ marshalledBytes[0] );
				return null;
		}
	}
}