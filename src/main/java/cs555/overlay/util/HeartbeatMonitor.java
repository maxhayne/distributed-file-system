package cs555.overlay.util;

import cs555.overlay.node.Controller;
import cs555.overlay.transport.ServerConnection;
import cs555.overlay.transport.ServerConnectionCache;

import java.util.ArrayList;
import java.util.Map;
import java.util.TimerTask;

public class HeartbeatMonitor extends TimerTask {

  private final Controller controller;
  private final ServerConnectionCache connectionCache;

  public HeartbeatMonitor(Controller controller,
      ServerConnectionCache connectionCache) {
    this.controller = controller;
    this.connectionCache = connectionCache;
  }

  private String[] splitFilename(String filename) {
    return filename.split( "_chunk|_shard" );
  }

  private String createStringOfFiles(ArrayList<FileMetadata> files) {
    if ( !files.isEmpty() ) {
      StringBuilder sb = new StringBuilder();
      for ( FileMetadata newFile : files ) {
        sb.append( newFile.filename ).append( ", " );
      }
      sb.delete( sb.length()-2, sb.length() );
      return "[ "+sb+" ]";
    }
    return "";
  }

  private long getTimeSinceLastHeartbeat(long now, HeartbeatInformation info) {
    return Math.min( now-info.getLastMajorHeartbeat(),
        now-info.getLastMinorHeartbeat() );
  }

  private int getLastHeartbeatType(long now, HeartbeatInformation info) {
    return now-info.getLastMajorHeartbeat() < now-info.getLastMinorHeartbeat() ?
               0 : 1;
  }

  private int calculateUnhealthyScore(long now, long connectionStartTime,
      HeartbeatInformation info) {
    int unhealthyScore = 0;
    // ChunkServer has missed a major heartbeat
    if ( info.getLastMajorHeartbeat() != -1 &&
         now-info.getLastMajorHeartbeat() > (Constants.HEARTRATE*11) ) {
      unhealthyScore += 1;
    }
    // ChunkServer has missed a minor heartbeat
    if ( info.getLastMinorHeartbeat() != -1 &&
         now-info.getLastMinorHeartbeat() > (Constants.HEARTRATE*2) ) {
      unhealthyScore += 1+( int ) (
          (now-info.getLastMinorHeartbeat()-(Constants.HEARTRATE*2))/
          Constants.HEARTRATE); // extra unhealthy for more
    }
    // Hasn't even sent a minor heartbeat
    if ( now-connectionStartTime > (Constants.HEARTRATE*2) &&
         info.getLastMinorHeartbeat() == -1 ) {
      unhealthyScore += 1;
    } // Hasn't even sent a major heartbeat
    if ( now-connectionStartTime > Constants.HEARTRATE &&
         info.getLastMajorHeartbeat() == -1 ) {
      unhealthyScore += 1;
    }
    return unhealthyScore;
  }

  private void adjustConnectionHealth(int unhealthyScore,
      ServerConnection connection) {
    if ( unhealthyScore >= 2 ) {
      connection.incrementUnhealthy();
    } else {
      connection.decrementUnhealthy();
    }
  }

  public synchronized void run() {
    ArrayList<Integer> toDeregister = new ArrayList<>();

    // THIS SHOULD NOW SYNCHRONIZE ON THE CONTROLLER ITSELF, SO THAT WE CAN
    // PREVENT CHUNKS FROM BEING ADDED OR SUBTRACTED, OR SERVERS FROM
    // REGISTERING OR DEREGISTERING WHILE WE ARE CONDUCTING ANALYSIS ON THE
    // HEARTBEATS
    synchronized( controller ) { // lock the Controller
      Map<Integer, ServerConnection> registeredServers =
          connectionCache.getRegisteredServers();
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

        // Append list of new files to print later
        sb.append( "\n" )
          .append( createStringOfFiles( heartbeatInformation.getFiles() ) );

        long timeSinceLastHeartbeat =
            getTimeSinceLastHeartbeat( now, heartbeatInformation );
        int lastBeatType = getLastHeartbeatType( now, heartbeatInformation );

        // Give the connection between the Controller and this ChunkServer a
        // score measuring how unhealthy it is.
        int unhealthyScore =
            calculateUnhealthyScore( now, connection.getStartTime(),
                heartbeatInformation );

        // Add a 'poke' message to send to the ChunkServer, and on
        // the next heartbeat, add to 'unhealthy' the discrepancy
        // between the 'pokes' and the 'poke replies'.

        // If unhealthyScore >= 2, increment connection's 'unhealthy' counter
        adjustConnectionHealth( unhealthyScore, connection );

        // If this node has failed 3 heartbeats in a row, deregister it
        if ( connection.getUnhealthy() > 3 ) {
          toDeregister.add( connection.getIdentifier() );
        }
      }

      // Print the contents of all heartbeats for registered ChunkServers
      System.out.println( sb.toString() );

      // For major heartbeats, the Controller check that there are no chunks
      // that should be held by the ChunkServer but aren't. It should then
      // try to replace those missing files by sending out repair messages.

      // Try to replace files missing from ChunkServers
      //ArrayList<String> missingFiles = idealState.differences(
      // reportedState );
      //System.out.println( missingFiles.size() );
      //      for ( String file : missingFiles )
      //        System.out.print( file + " ");
      /*
      for ( String missingFile : missingFiles ) {
        // missingFile structure: "timestamp,baseFilename,sequence,
        // serverIdentifier, (optional) fragment"
        String[] missingFileSplit = missingFile.split( "," );

        // Decode what was returned
        long timestamp = Long.parseLong( missingFileSplit[0] );
        String baseFilename = missingFileSplit[1];
        int sequence = Integer.parseInt( missingFileSplit[2] );
        int serverIdentifier = Integer.parseInt( missingFileSplit[3] );
        int fragment = missingFileSplit.length == 5 ?
                           Integer.parseInt( missingFileSplit[4] ) : -1;

        // if chunk is newly created,
        if ( now-timestamp < Constants.HEARTRATE ) {
          continue; // If the chunk is new, might just not have been stored
          // yet.
        }

        String serverAddress =
            connectionCache.getChunkServerAddress( serverIdentifier );
        if ( serverAddress.isEmpty() ) {
          continue;
        }

        String storageInfo =
            connectionCache.getChunkStorageInfo( baseFilename, sequence );

        Event repairMessage;
        String addressToContact;
        String filename;
        if ( missingFileSplit.length == 4 ) { // It is a chunk
          filename = baseFilename+"_chunk"+sequence;
          String[] servers = storageInfo.split( "\\|", -1 )[0].split( "," );
          // Remove destination server from list of servers
          servers = ArrayUtilities.removeFromArray( servers, serverAddress );
          RepairChunk repairChunk = new RepairChunk( filename, serverAddress,
              new int[]{ 0, 1, 2, 3, 4, 5, 6, 7 }, servers );
          addressToContact = repairChunk.getAddress();
          repairMessage = repairChunk;
        } else { // It is a shard
          filename = baseFilename+"_chunk"+sequence+"_shard"+fragment;
          String[] servers = storageInfo.split( "\\|", -1 )[1].split( "," );
          if ( servers.length == 1 && servers[0].isEmpty() ) {
            continue;
          }
          // Replace "-1" with null in shard server array
          ArrayUtilities.replaceArrayItem( servers, "-1", null );
          RepairShard repairShard =
              new RepairShard( filename, serverAddress, servers );
          addressToContact = repairShard.getAddress();
          repairMessage = repairShard;
        }

        try {
          connectionCache.getConnection( addressToContact )
                         .getConnection()
                         .getSender()
                         .sendData( repairMessage.getBytes() );
        } catch ( Exception e ) {
          System.err.println( "HearbeatMonitor::run: There was a problem "+
                              "sending the repair request to "+addressToContact+
                              " to replace '"+filename+"' at "+serverAddress+
                              "."+e.getMessage() );
        }
      }
      */
    }
    for ( Integer i : toDeregister ) {
      controller.deregister( i );
    }
  }
}