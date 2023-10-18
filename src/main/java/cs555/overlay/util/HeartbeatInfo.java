package cs555.overlay.util;

import java.util.ArrayList;

/**
 * Class used to store information from the latest heartbeat sent by the
 * ChunkServer in whose ChunkServerConnection this object is stored.
 *
 * @author hayne
 */
public class HeartbeatInfo {

  private long lastMajorHeartbeat;
  private long lastMinorHeartbeat;

  private long freeSpace;
  private int totalChunks;
  private ArrayList<FileMetadata> files;

  public HeartbeatInfo() {
    this.lastMajorHeartbeat = -1;
    this.lastMinorHeartbeat = -1;
    this.freeSpace = -1;
    this.totalChunks = -1;
    this.files = new ArrayList<>();
  }

  public synchronized void update(int type, long freeSpace, int totalChunks,
      ArrayList<FileMetadata> files) {
    if ( type == 1 ) {
      lastMajorHeartbeat = System.currentTimeMillis();
    } else {
      lastMinorHeartbeat = System.currentTimeMillis();
    }
    this.freeSpace = freeSpace;
    this.totalChunks = totalChunks;
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
    return new ArrayList<FileMetadata>( files );
  }

  public synchronized HeartbeatInfo copy() {
    HeartbeatInfo copy = new HeartbeatInfo();
    copy.lastMajorHeartbeat = lastMajorHeartbeat;
    copy.lastMinorHeartbeat = lastMinorHeartbeat;
    copy.files = files;
    copy.freeSpace = freeSpace;
    copy.totalChunks = totalChunks;
    return copy;
  }
}