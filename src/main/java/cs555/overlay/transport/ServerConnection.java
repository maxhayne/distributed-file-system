package cs555.overlay.transport;

import cs555.overlay.util.HeartbeatInformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains an active TCPConnection with a ChunkServer. Its function is to
 * maintain status and heartbeat information about the ChunkServer it's
 * connected to.
 *
 * @author hayne
 */
public class ServerConnection {

  // Identifying/Connection information
  private final String address;
  private final int identifier;
  private final Map<String,ArrayList<Integer>> storedChunks;

  // HeartbeatInformation
  private final HeartbeatInformation heartbeatInformation;
  private final long startTime;
  private int unhealthy;
  private int pokes;
  private int pokeReplies;

  public ServerConnection(int identifier, String address) {
    this.identifier = identifier;
    this.address = address;
    this.storedChunks = new HashMap<>();
    this.heartbeatInformation = new HeartbeatInformation();
    this.unhealthy = 0;
    this.pokes = 0;
    this.pokeReplies = 0;
    this.startTime = System.currentTimeMillis();
  }

  /**
   * Returns the list of chunks supposedly to be stored by this server. Will
   * only be called upon deregistration.
   *
   * @return storedChunks HashMap
   */
  public Map<String,ArrayList<Integer>> getStoredChunks() {
    return storedChunks;
  }

  /**
   * Adds a 'sequence' number to the ArrayList mapped to by 'filename' in the
   * 'storedChunks' map.
   *
   * @param filename filename of file
   * @param sequence sequence number of chunk
   */
  public void addChunk(String filename, int sequence) {
    synchronized(storedChunks) {
      ArrayList<Integer> sequenceNumbers = storedChunks.get(filename);
      if (sequenceNumbers == null) {
        storedChunks.put(filename, new ArrayList<>(List.of(sequence)));
      } else {
        sequenceNumbers.add(sequence);
      }
    }
  }

  /**
   * Deletes an integer from the ArrayList of sequence numbers for a particular
   * file in the storedChunks map.
   *
   * @param filename filename of file
   * @param sequence sequence of file to be removed
   */
  public void removeChunk(String filename, int sequence) {
    synchronized(storedChunks) {
      ArrayList<Integer> sequenceNumbers = storedChunks.get(filename);
      if (sequenceNumbers != null) {
        sequenceNumbers.remove(Integer.valueOf(sequence));
        if (sequenceNumbers.isEmpty()) {
          deleteFile(filename);
        }
      }
    }
  }

  /**
   * Removes a file with filename from the storedChunks map.
   *
   * @param filename filename of file
   */
  public void deleteFile(String filename) {
    synchronized(storedChunks) {
      storedChunks.remove(filename);
    }
  }

  public synchronized void incrementPokes() {
    pokes++;
  }

  public synchronized void incrementPokeReplies() {
    pokeReplies++;
  }

  public synchronized int pokeDiscrepancy() {
    return (pokes - pokeReplies);
  }

  public HeartbeatInformation getHeartbeatInfo() {
    return heartbeatInformation;
  }

  public long getFreeSpace() {
    return heartbeatInformation.getFreeSpace();
  }

  public int getTotalChunks() {
    return heartbeatInformation.getTotalChunks();
  }

  // Total chunks in local storedChunks map
  public int totalStoredChunks() {
    int totalChunks = 0;
    synchronized(storedChunks) {
      for (ArrayList<Integer> fileChunks : storedChunks.values()) {
        totalChunks += fileChunks.size();
      }
    }
    return totalChunks;
  }

  public long getStartTime() {
    return startTime;
  }

  public int getIdentifier() {
    return identifier;
  }

  public String getServerAddress() {
    return address;
  }

  public synchronized int getUnhealthy() {
    return unhealthy;
  }

  public synchronized void incrementUnhealthy() {
    this.unhealthy += 1;
  }

  public synchronized void decrementUnhealthy() {
    if (unhealthy > 0) {
      unhealthy -= 1;
    }
  }

  public synchronized String toString() {
    return identifier + ", " + address + ", " +
           heartbeatInformation.getFreeSpace()/(1024*1024) + "MB, " +
           heartbeatInformation.getTotalChunks() + " chunks, " + "health: " +
           unhealthy;
  }

}