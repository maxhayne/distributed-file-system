package cs555.overlay.wireformats;

import cs555.overlay.util.Logger;

import java.io.IOException;

/**
 * Factory to create the type of event which is received in a TCPReceiverThread.
 * Implemented as singleton. To, in theory, save space.
 *
 * @author hayne
 */
public class EventFactory {

  private static final Logger logger = Logger.getInstance();
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
  public static EventFactory getInstance() {
    return eventFactory;
  }

  /**
   * Create new event by inspecting the message type in the first location of
   * the byte message.
   *
   * @param marshalledBytes of received message
   * @return event created by unmarshalling bytes
   */
  public Event createEvent(byte[] marshalledBytes) throws IOException {
    switch ( marshalledBytes[0] ) {
      case Protocol.CHUNK_SERVER_SENDS_REGISTRATION:
      case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION:
      case Protocol.CONTROLLER_DENIES_STORAGE_REQUEST:
      case Protocol.CONTROLLER_SENDS_HEARTBEAT:
      case Protocol.CLIENT_REQUESTS_FILE_LIST:
      case Protocol.CONTROLLER_APPROVES_FILE_DELETE:
      case Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO:
      case Protocol.CLIENT_REQUESTS_FILE_DELETE:
      case Protocol.REQUEST_FILE:
      case Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_FOR_STORAGE:
      case Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS:
      case Protocol.CHUNK_SERVER_DENIES_REQUEST:
      case Protocol.CONTROLLER_REQUESTS_FILE_DELETE:
      case Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE:
        return new GeneralMessage( marshalledBytes );

      case Protocol.CONTROLLER_SENDS_STORAGE_LIST:
        return new ControllerSendsStorageList( marshalledBytes );

      case Protocol.CONTROLLER_SENDS_FILE_LIST:
        return new ControllerSendsFileList( marshalledBytes );

      case Protocol.CONTROLLER_RESERVES_SERVERS:
        return new ControllerReservesServers( marshalledBytes );

      case Protocol.CLIENT_STORE:
        return new ClientStore( marshalledBytes );

      case Protocol.REPAIR_CHUNK:
        return new RepairChunk( marshalledBytes );

      case Protocol.REPAIR_SHARD:
        return new RepairShard( marshalledBytes );

      case Protocol.SENDS_FILE_FOR_STORAGE:
        return new SendsFileForStorage( marshalledBytes );

      case Protocol.CHUNK_SERVER_SERVES_FILE:
        return new ChunkServerServesFile( marshalledBytes );

      case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT:
        return new ChunkServerSendsHeartbeat( marshalledBytes );

      case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT:
        return new ChunkServerRespondsToHeartbeat( marshalledBytes );

      case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION:
        return new ChunkServerReportsFileCorruption( marshalledBytes );

      default:
        logger.error( "Event couldn't be created. "+marshalledBytes[0] );
        return null;
    }
  }
}