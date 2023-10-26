package cs555.overlay.transport;

import cs555.overlay.util.HeartbeatInformation;

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
  private final TCPConnection connection;

  private final long startTime;
  // HeartbeatInformation
  private final HeartbeatInformation heartbeatInformation;
  // latest heartbeat info
  private int unhealthy;
  private int pokes;
  private int pokeReplies;

  public ServerConnection(int identifier, String address,
      TCPConnection connection) {
    this.identifier = identifier;
    this.address = address;
    this.connection = connection;
    this.heartbeatInformation = new HeartbeatInformation();

    this.startTime = System.currentTimeMillis();
    this.unhealthy = 0;
    this.pokes = 0;
    this.pokeReplies = 0;
  }

  public synchronized void incrementPokes() {
    pokes++;
  }

  public synchronized void incrementPokeReplies() {
    pokeReplies++;
  }

  public synchronized int getPokeDiscrepancy() {
    return (pokes-pokeReplies);
  }

  public HeartbeatInformation getHeartbeatInfo() {
    return heartbeatInformation;
  }

  public long getFreeSpace() {return heartbeatInformation.getFreeSpace();}
  public int getTotalChunks() {return heartbeatInformation.getTotalChunks();}

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
    if ( unhealthy > 0 ) {
      unhealthy -= 1;
    }
  }

  public TCPConnection getConnection() {
    return connection;
  }

  public synchronized String toString() {
    String info = address+", "+identifier+", "+
                  heartbeatInformation.getFreeSpace()/(1024*1024)+"MB"+", "+
                  heartbeatInformation.getTotalChunks()+" chunks"+", "+
                  "health: "+unhealthy;
    return "[ "+info+" ]";
  }

}