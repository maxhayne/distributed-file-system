package cs555.overlay.util;

import cs555.overlay.node.ChunkServer;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;
import cs555.overlay.wireformats.Event;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to be encapsulated in a TimerTask to be called by the ChunkServer.
 *
 * @author hayne
 */
public class HeartbeatService extends TimerTask {
  private static final Logger logger = Logger.getInstance();
  private final ChunkServer chunkServer;
  private final ConcurrentHashMap<String,FileMetadata> majorFiles;
  private final ConcurrentHashMap<String,FileMetadata> minorFiles;
  private int round;
  private final TCPConnectionCache connectionCache; // just for Controller

  public HeartbeatService(ChunkServer chunkServer) {
    this.chunkServer = chunkServer;
    this.majorFiles = new ConcurrentHashMap<>();
    this.minorFiles = new ConcurrentHashMap<>();
    this.round = 1;
    this.connectionCache = new TCPConnectionCache(chunkServer);
  }

  /**
   * Constructs a heartbeat message using the files stored in the 'files'
   * FileMap member of the ChunkServer.
   *
   * @param beatType 1 for major, 0 for minor
   * @return a heartbeat message object
   */
  private Event heartbeat(int beatType) {
    // fileMap is the map that we are modifying in this heartbeat.
    // minorHeartbeat fileMap = minorFiles, majorHeartbeat fileMap = majorFiles
    Map<String,FileMetadata> fileMap = beatType == 0 ? minorFiles : majorFiles;
    fileMap.clear(); // will be filled up during this heartbeat
    // Use files map in ChunkServer to construct message
    chunkServer.getFiles().getMap().forEach(1000, (filename, record) -> {
      if ((beatType == 0 && !majorFiles.containsKey(filename)) ||
          beatType == 1) {
        fileMap.put(filename, record.md());
      }
    });
    if (beatType == 0) { // add minorFiles to majorFiles
      majorFiles.putAll(fileMap);
    }
    // Create heartbeat message, and return bytes of that message
    return generateHeartbeatMessage(beatType, fileMap);
  }

  /**
   * Creates a heartbeat message.
   *
   * @param beatType type of heartbeat
   * @param fileMap map being used (majorFiles or minorFiles)
   * @return the heartbeat message to be sent
   */
  private ChunkServerSendsHeartbeat generateHeartbeatMessage(int beatType,
      Map<String,FileMetadata> fileMap) {
    int totalChunks = majorFiles.size();
    long freeSpace = chunkServer.getStreamer().usableSpace();
    // Even if a FileMetadata is removed from the FileMap before the message
    // is created, the reference will still exist for our usage
    return new ChunkServerSendsHeartbeat(chunkServer.getIdentifier(), beatType,
        totalChunks, freeSpace, new ArrayList<>(fileMap.values()));
  }

  /**
   * The method that will be run every time the ChunkServer is scheduled to
   * perform a heartbeat.
   */
  public synchronized void run() {
    Event heartbeat = round%10 == 0 ? heartbeat(1) : heartbeat(0);
    if (connectionCache.send(chunkServer.getControllerAddress(), heartbeat,
        false, false)) {
      logger.debug("Heartbeat " + round + " sent at " + new Date());
    } else {
      logger.debug("Unable to send heartbeat " + round + " to Controller. ");
    }
    round++;
  }
}