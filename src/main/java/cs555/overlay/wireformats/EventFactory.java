package cs555.overlay.wireformats;

public class EventFactory {
	public static Event getEvent(byte eventType, byte[] msg) {
		switch(eventType) {
			case Protocol.CHUNK_SERVER_SENDS_REGISTRATION: {
				return new ChunkServerSendsRegistration(msg);
			}

			case Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS: {
				return new ControllerReportsChunkServerRegistrationStatus(msg);
			}

			case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION: {
				return new ChunkServerSendsDeregistration(msg);
			}

			case Protocol.CLIENT_REQUESTS_STORE_CHUNK: {
				return new ClientRequestsStoreChunk(msg);
			}

			case Protocol.CONTROLLER_SENDS_CLIENT_VALID_CHUNK_SERVERS: {
				return new ControllerSendsClientValidChunkServers(msg);
			}

			case Protocol.CONTROLLER_DENIES_STORAGE_REQUEST: {
				return new ControllerDeniesStorageRequest();
			}

			case Protocol.CLIENT_REQUESTS_STORE_SHARDS: {
				return new ClientRequestsStoreShards(msg);
			}

			case Protocol.CONTROLLER_SENDS_CLIENT_VALID_SHARD_SERVERS: {
				return new ControllerSendsClientValidShardServers(msg);
			}

			case Protocol.CLIENT_REQUESTS_FILE_DELETE: {
				return new ClientRequestsFileDelete(msg);
			}

			case Protocol.CONTROLLER_APPROVES_FILE_DELETE: {
				return new ControllerApprovesFileDelete();
			}

			case Protocol.CONTROLLER_REQUESTS_FILE_DELETE: {
				return new ControllerRequestsFileDelete(msg);
			}

			case Protocol.SENDS_FILE_FOR_STORAGE: {
				return new SendsFileForStorage(msg);
			}

			case Protocol.REQUESTS_CHUNK: {
				return new RequestsChunk(msg);
			}

			case Protocol.REQUESTS_SHARD: {
				return new RequestsShard(msg);
			}

			case Protocol.CHUNK_SERVER_DENIES_REQUEST: {
				return new ChunkServerDeniesRequest(msg);
			}

			case Protocol.CHUNK_SERVER_SERVES_FILE: {
				return new ChunkServerServesFile(msg);
			}

			case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT: {
				return new ChunkServerSendsHeartbeat(msg);
			}

			case Protocol.CONTROLLER_SENDS_HEARTBEAT: {
				return new ControllerSendsHeartbeat();
			}

			case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT: {
				return new ChunkServerRespondsToHeartbeat();
			}

			case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION: {
				return new ChunkServerReportsFileCorruption(msg);
			}

			case Protocol.CONTROLLER_SENDS_STORAGE_LIST: {
				return new ControllerSendsStorageList(msg);
			}

			case Protocol.CONTROLLER_REQUESTS_FILE_FORWARD: {
				return new ControllerRequestsFileForward(msg);
			}
		}
		return null;
	}
}