package cs555.overlay.util;

import cs555.overlay.node.Controller;
import cs555.overlay.transport.ControllerInformation;
import cs555.overlay.transport.ServerConnection;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RepairShard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.TimerTask;

/**
 * Is run on a timer schedule for every Constants.HEARTRATE milliseconds. Reads
 * the latest heartbeat sent by each registered ChunkServer and prints out
 * information to the terminal. Tries to detect whether a ChunkServer is failing
 * and should be deregistered. On major heartbeats, tries to replace files at
 * servers that are missing.
 *
 * @author hayne
 */
public class HeartbeatMonitor extends TimerTask {

  private final Controller controller;
  private final ControllerInformation information;

  public HeartbeatMonitor(Controller controller,
      ControllerInformation information) {
    this.controller = controller;
    this.information = information;
  }

  /**
   * Creates a long String of filenames sent by the ChunkServer in the most
   * recent heartbeat.
   *
   * @param files ArrayList of FileMetadatas from latest heartbeat message
   * @return String of files
   */
  private String createStringOfFiles(ArrayList<FileMetadata> files) {
    if ( !files.isEmpty() ) {
      StringBuilder sb = new StringBuilder();
      for ( FileMetadata newFile : files ) {
        sb.append( newFile.getFilename() ).append( ", " );
      }
      sb.delete( sb.length()-2, sb.length() );
      return "[ "+sb+" ]";
    }
    return "";
  }

  /**
   * Deduces the type of the last heartbeat received by the Controller from a
   * ChunkServer
   *
   * @param now current time
   * @param info latest heartbeat info from server
   * @return 0 for minor, 1 for major, -1 for neither
   */
  private int getLastHeartbeatType(long now, HeartbeatInformation info) {
    if ( info.getLastMinorHeartbeat() == 0 &&
         info.getLastMajorHeartbeat() == 0 ) {
      return -1;
    }
    return now-info.getLastMajorHeartbeat() < now-info.getLastMinorHeartbeat() ?
               1 : 0;
  }

  /**
   * Produces a score of how unhealthy the ChunkServer is based on a few
   * criteria.
   *
   * @param now current time
   * @param connectionStartTime time when ChunkServer registered
   * @param info latest heartbeat info from server
   * @return unhealthy score
   */
  private int calculateUnhealthyScore(long now, long connectionStartTime,
      HeartbeatInformation info) {
    int unhealthyScore = 0;
    // ChunkServer has missed a major heartbeat
    if ( info.getLastMajorHeartbeat() != 0 &&
         now-info.getLastMajorHeartbeat() > (Constants.HEARTRATE*11) ) {
      unhealthyScore++;
    }
    // ChunkServer has missed a minor heartbeat
    if ( info.getLastMinorHeartbeat() != 0 &&
         now-info.getLastMinorHeartbeat() > (Constants.HEARTRATE*2) ) {
      unhealthyScore += 1+( int ) (
          (now-info.getLastMinorHeartbeat()-(Constants.HEARTRATE*2))/
          Constants.HEARTRATE); // extra unhealthy for more
    }
    // Hasn't even sent a minor heartbeat
    if ( now-connectionStartTime > (Constants.HEARTRATE*2) &&
         info.getLastMinorHeartbeat() == 0 ) {
      unhealthyScore += 1;
    } // Hasn't even sent a major heartbeat
    if ( now-connectionStartTime > Constants.HEARTRATE &&
         info.getLastMajorHeartbeat() == 0 ) {
      unhealthyScore += 1;
    }
    return unhealthyScore;
  }

  /**
   * Adjusts the unhealthy member of the ServerConnection.
   *
   * @param unhealthyScore
   * @param connection server to adjust connection health for
   */
  private void adjustConnectionHealth(int unhealthyScore,
      ServerConnection connection) {
    if ( unhealthyScore >= 2 ) {
      connection.incrementUnhealthy();
    } else {
      connection.decrementUnhealthy();
    }
  }

  /**
   * If the major heartbeat doesn't contain a file that the Controller believes
   * it should, it tries to dispatch a repair message to replace it.
   *
   * @param connection ServerConnection whose files to cross-reference
   * @param information HeartbeatInformation for this connection
   */
  private void replaceMissingFiles(ServerConnection connection,
      HeartbeatInformation information) {
    HashSet<String> chunksAtServer = new HashSet<>();
    for ( String filename : information.getFiles()
                                       .stream()
                                       .map( FileMetadata::getFilename )
                                       .toList() ) {
      chunksAtServer.add( filename.split( "_shard" )[0] );
    }
    Map<String, ArrayList<Integer>> storedChunks = connection.getStoredChunks();
    for ( String filename : storedChunks.keySet() ) {
      ArrayList<Integer> sequences = storedChunks.get( filename );
      for ( int sequence : sequences ) {
        if ( !chunksAtServer.contains( filename+"_chunk"+sequence ) ) {
          System.out.println(
              "Dispatching repair for "+filename+"_chunk"+sequence );
          dispatchRepair( filename, sequence, connection.getServerAddress() );
        }
      }
    }
  }

  /**
   * Creates a repair message to replace the chunk with the filename/sequence
   * combo given as parameters. A different message has to be created whether
   * erasure coding or replicating.
   *
   * @param filename base filename of chunk to be repaired
   * @param sequence sequence of chunk to be repaired
   * @param destination host:port address of server that needs replacement file
   */
  private void dispatchRepair(String filename, int sequence,
      String destination) {
    String[] servers = information.getServers( filename, sequence );
    Event repairMessage;
    String sendTo;
    if ( ApplicationProperties.storageType.equals( "erasure" ) ) {
      RepairShard repairShard =
          new RepairShard( filename+"_chunk"+sequence+"_shard", destination,
              servers );
      sendTo = repairShard.getAddress();
      repairMessage = repairShard;
    } else {
      RepairChunk repairChunk =
          new RepairChunk( filename+"_chunk"+sequence, destination,
              new int[]{ 0, 1, 2, 3, 4, 5, 6, 7 },
              ArrayUtilities.removeFromArray( servers, destination ) );
      sendTo = repairChunk.getAddress();
      repairMessage = repairChunk;
    }
    try {
      information.getConnection( sendTo )
                 .getConnection()
                 .getSender()
                 .sendData( repairMessage.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "dispatchRepair: Failed to send message. "+ioe.getMessage() );
    }
  }

  /**
   * Method performed every time the HeartbeatMonitor's timer task is called.
   */
  public void run() {
    ArrayList<Integer> toDeregister = new ArrayList<>();
    // synchronized on Controller itself to prevent modification operations
    synchronized( controller ) { // lock the Controller
      Map<Integer, ServerConnection> registeredServers =
          information.getRegisteredServers();
      if ( registeredServers.isEmpty() ) {
        return; // no information to report
      }

      StringBuilder sb = new StringBuilder();
      sb.append( "\nHeartbeat Monitor:" );

      long now = System.currentTimeMillis();
      for ( ServerConnection connection : registeredServers.values() ) {
        sb.append( "\n" ).append( connection.toString() );

        HeartbeatInformation heartbeatInformation =
            connection.getHeartbeatInfo().copy();

        sb.append( "\n" )
          .append( createStringOfFiles( heartbeatInformation.getFiles() ) );

        int unhealthyScore =
            calculateUnhealthyScore( now, connection.getStartTime(),
                heartbeatInformation );
        adjustConnectionHealth( unhealthyScore, connection );
        if ( connection.getUnhealthy() > 3 ) {
          toDeregister.add( connection.getIdentifier() );
          continue;
        }

        // For a major heartbeat, replace missing files
        int lastBeatType = getLastHeartbeatType( now, heartbeatInformation );
        if ( lastBeatType == 1 ) {
          replaceMissingFiles( connection, heartbeatInformation );
        }
      }

      // Print the contents of all heartbeats for registered ChunkServers
      System.out.println( sb );
    }
    for ( Integer i : toDeregister ) {
      controller.deregister( i );
    }
  }
}