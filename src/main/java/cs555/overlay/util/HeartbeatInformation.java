package cs555.overlay.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

/**
 * Class used to store information from the latest heartbeat sent by the
 * ChunkServer in whose ServerConnection this object is stored.
 *
 * @author hayne
 */
public class HeartbeatInformation {

  private long lastMajorHeartbeat; // time when last major heartbeat received
  private long lastMinorHeartbeat; // time when last minor heartbeat received
  private long freeSpace; // in bytes
  private int totalChunks;
  private ArrayList<FileMetadata> files;
  private HashSet<Map.Entry<String, Integer>> missingChunks;

  public HeartbeatInformation() {
    this.lastMajorHeartbeat = 0;
    this.lastMinorHeartbeat = 0;
    this.freeSpace = 0;
    this.totalChunks = 0;
    this.files = new ArrayList<>();
    this.missingChunks = new HashSet<>();
  }

  public synchronized void update(int type, long freeSpace, int totalChunks,
      ArrayList<FileMetadata> files) {
    this.freeSpace = freeSpace;
    this.totalChunks = totalChunks;
    if ( type == 1 ) {
      lastMajorHeartbeat = System.currentTimeMillis();
    } else {
      lastMinorHeartbeat = System.currentTimeMillis();
    }
    this.files = files;
  }

  public synchronized long getLastMajorHeartbeat() {
    return lastMajorHeartbeat;
  }

  public synchronized long getLastMinorHeartbeat() {
    return lastMinorHeartbeat;
  }

  public synchronized long getFreeSpace() {
    return freeSpace;
  }

  public synchronized int getTotalChunks() {
    return totalChunks;
  }

  public synchronized ArrayList<FileMetadata> getFiles() {
    return new ArrayList<>( files );
  }

  public synchronized HashSet<Map.Entry<String, Integer>> getMissingChunks() {
    return missingChunks;
  }

  public synchronized HeartbeatInformation copy() {
    HeartbeatInformation copy = new HeartbeatInformation();
    copy.lastMajorHeartbeat = lastMajorHeartbeat;
    copy.lastMinorHeartbeat = lastMinorHeartbeat;
    copy.files = files;
    copy.freeSpace = freeSpace;
    copy.totalChunks = totalChunks;
    copy.missingChunks = missingChunks; // only modified by HeartbeatMonitor
    return copy;
  }
}