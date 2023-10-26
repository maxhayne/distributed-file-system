package cs555.overlay.transport;

import cs555.overlay.util.*;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RepairShard;

import java.io.IOException;
import java.util.*;

public class ServerConnectionCache {

  private final ArrayList<Integer> availableIdentifiers;
  private final Map<Integer, ServerConnection> registeredServers;

  private final DistributedFileCache idealState;
  private final DistributedFileCache reportedState;

  private static final Comparator<ServerConnection> serverComparator =
      Comparator.comparingInt( ServerConnection::getTotalChunks )
                .thenComparing( ServerConnection::getFreeSpace,
                    Comparator.reverseOrder() );

  public ServerConnectionCache(DistributedFileCache idealState,
      DistributedFileCache reportedState) {
    this.idealState = idealState;
    this.reportedState = reportedState;

    this.registeredServers = new HashMap<Integer, ServerConnection>();
    this.availableIdentifiers = new ArrayList<Integer>();
    for ( int i = 1; i <= 32; ++i ) {
      this.availableIdentifiers.add( i );
    }

    HeartbeatMonitor heartbeatMonitor =
        new HeartbeatMonitor( this, registeredServers, idealState,
            reportedState );

    Timer heartbeatTimer = new Timer();
    heartbeatTimer.scheduleAtFixedRate( heartbeatMonitor, 0,
        Constants.HEARTRATE );
  }

  /**
   * Returns the ServerConnection object of a registered ChunkServer with
   * the identifier specified as a parameter.
   *
   * @param identifier of ChunkServer
   * @return ServerConnection with that identifier, null if doesn't exist
   */
  public ServerConnection getConnection(int identifier) {
    synchronized( registeredServers ) {
      return registeredServers.get( identifier );
    }
  }

  /**
   * Return the ServerConnection object of a registered ChunkServer with
   * the host:port address specified as a parameter.
   *
   * @param address of ChunkServer's connection to get
   * @return ServerConnection
   */
  public ServerConnection getConnection(String address) {
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( connection.getServerAddress().equals( address ) ) {
          return connection;
        }
      }
    }
    return null;
  }

  public DistributedFileCache getIdealState() {
    return idealState;
  }

  public DistributedFileCache getReportedState() {
    return reportedState;
  }

  public String[] getAllServerAddresses() {
    synchronized( registeredServers ) {
      String[] addresses = new String[registeredServers.size()];
      int index = 0;
      for ( ServerConnection connection : registeredServers.values() ) {
        addresses[index] = connection.getServerAddress();
        index++;
      }
      return addresses;
    }
  }

  public int getChunkServerIdentifier(String address) {
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( connection.getServerAddress().equals( address ) ) {
          return connection.getIdentifier();
        }
      }
    }
    return -1;
  }

  public String getChunkServerAddress(int identifier) {
    synchronized( registeredServers ) {
      ServerConnection connection = registeredServers.get( identifier );
      if ( connection != null ) {
        return connection.getServerAddress();
      }
    }
    return "";
  }

  public String getChunkStorageInfo(String filename, int sequence) {
    //String info = reportedState.getChunkStorageInfo( filename, sequence );
    String info = idealState.getChunkStorageInfo( filename, sequence );
    if ( info.equals( "|" ) ) {
      return "|";
    }
    String[] parts = info.split( "\\|", -1 );
    StringBuilder sb = new StringBuilder();
    if ( !parts[0].isEmpty() ) {
      String[] replications = parts[0].split( "," );
      boolean added = false;
      for ( String replication : replications ) {
        String address =
            getChunkServerAddress( Integer.parseInt( replication ) );
        if ( !address.isEmpty() ) {
          sb.append( address ).append( "," );
          added = true;
        }
      }
      if ( added ) {
        sb.deleteCharAt( sb.length()-1 );
      }
    }
    sb.append( "|" );
    if ( !parts[1].isEmpty() ) {
      String[] shardServers = parts[1].split( "," );
      for ( String shardServer : shardServers ) {
        if ( !shardServer.equals( "-1" ) ) {
          String address =
              getChunkServerAddress( Integer.parseInt( shardServer ) );
          if ( !address.isEmpty() ) {
            sb.append( address ).append( "," );
          } else {
            sb.append( "-1" ).append( "," );
          }
        } else {
          sb.append( "-1" ).append( "," );
        }
      }
      sb.deleteCharAt( sb.length()-1 );
    }
    return sb.toString();
  }

  public ArrayList<String> sortServers() {
    synchronized( registeredServers ) {
      List<ServerConnection> orderedServers =
          new ArrayList<>( registeredServers.values() );
      orderedServers.sort( serverComparator );
      ArrayList<String> orderedAddresses = new ArrayList<>();
      for ( ServerConnection server : orderedServers ) {
        orderedAddresses.add( server.getServerAddress() );
      }
      return orderedAddresses;
    }
  }

  public ArrayList<String> listFreestServers() {
    ArrayList<Long[]> servers = new ArrayList<Long[]>();
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( connection.getUnhealthy() <= 3 &&
             connection.getHeartbeatInfo().getFreeSpace() != -1 &&
             connection.getHeartbeatInfo().getFreeSpace() >= 65720 ) {
          servers.add( new Long[]{ connection.getHeartbeatInfo().getFreeSpace(),
              ( long ) connection.getIdentifier() } );
        }
      }
    }

    servers.sort( Comparators.SERVER_SORT );
    Collections.reverse( servers );

    ArrayList<String> freestServers = new ArrayList<String>();
    for ( Long[] server : servers ) {
      freestServers.add( String.valueOf( server[1] ) );
    }
    return freestServers;
  }

  // Return the best ChunkServers in terms of storage
  public String availableChunkServers(String filename, int sequence) {
    // Need three freest servers
    ArrayList<Long[]> servers = new ArrayList<Long[]>();
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( connection.getUnhealthy() <= 3 &&
             connection.getHeartbeatInfo().getFreeSpace() != -1 &&
             connection.getHeartbeatInfo().getFreeSpace() >= 65720 ) {
          servers.add( new Long[]{ connection.getHeartbeatInfo().getFreeSpace(),
              ( long ) connection.getIdentifier() } );
        }
      }
    }

    servers.sort( Comparators.SERVER_SORT );
    Collections.reverse( servers );

    synchronized( idealState ) { // If already allocated, return the same
      // three servers
      if ( !idealState.getChunkStorageInfo( filename, sequence )
                      .split( "\\|", -1 )[0].isEmpty() ) {
        String[] temp = idealState.getChunkStorageInfo( filename, sequence )
                                  .split( "\\|", -1 )[0].split( "," );
        StringBuilder sb = new StringBuilder();
        for ( String server : temp ) {
          sb.append( getChunkServerAddress( Integer.parseInt( server ) ) )
            .append( "," );
        }
        sb.deleteCharAt( sb.length()-1 );
        return sb.toString();
      }
      if ( servers.size() < 3 ) {
        return "";
      }
      StringBuilder sb = new StringBuilder();
      for ( int i = 0; i < 3; i++ ) {
        Chunk chunk =
            new Chunk( filename, sequence, 0, System.currentTimeMillis(),
                ( int ) ( long ) servers.get( i )[1], false );
        idealState.addChunk( chunk );
        sb.append(
              getChunkServerAddress( ( int ) ( long ) servers.get( i )[1] ) )
          .append( "," );
      }
      sb.deleteCharAt( sb.length()-1 );
      return sb.toString();
    }
  }

  // Return the best ChunkServers in terms of storage
  public String availableShardServers(String filename, int sequence) {
    // Need nine freest servers
    ArrayList<Long[]> servers = new ArrayList<Long[]>();
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( connection.getUnhealthy() <= 3 &&
             connection.getHeartbeatInfo().getFreeSpace() != -1 &&
             connection.getHeartbeatInfo().getFreeSpace() >= 65720 ) {
          servers.add( new Long[]{ connection.getHeartbeatInfo().getFreeSpace(),
              ( long ) connection.getIdentifier() } );
        }
      }
    }

    servers.sort( Comparators.SERVER_SORT );
    Collections.reverse( servers );

    synchronized( idealState ) { // If already allocated, return the same
      // three servers
      if ( !idealState.getChunkStorageInfo( filename, sequence )
                      .split( "\\|", -1 )[1].isEmpty() ) {
        String[] temp = idealState.getChunkStorageInfo( filename, sequence )
                                  .split( "\\|", -1 )[1].split( "," );
        StringBuilder sb = new StringBuilder();
        for ( String server : temp ) {
          if ( server.equals( "-1" ) ) {
            sb.append( "-1" );
            continue;
          }
          sb.append( Integer.parseInt( server ) ).append( "," );
        }
        sb.deleteCharAt( sb.length()-1 );
        return sb.toString();
      }
      if ( servers.size() < 9 ) {
        return "";
      }
      StringBuilder sb = new StringBuilder();
      for ( int i = 0; i < 9; i++ ) {
        Shard shard =
            new Shard( filename, sequence, i, 0, System.currentTimeMillis(),
                ( int ) ( long ) servers.get( i )[1], false );
        idealState.addShard( shard );
        sb.append(
              getChunkServerAddress( ( int ) ( long ) servers.get( i )[1] ) )
          .append( "," );
      }
      sb.deleteCharAt( sb.length()-1 );
      return sb.toString();
    }
  }

  /**
   * Returns whether particular host:port address has registered as a
   * ChunkServer.
   *
   * @param address
   * @return true if registered, false if not
   */
  public boolean isRegistered(String address) {
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( address.equals( connection.getServerAddress() ) ) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Attempts to register the host:port combination as a ChunkServer. If
   * registration fails, returns -1, else it returns the identifier.
   *
   * @return status of registration attempt
   */
  public int register(String address, TCPConnection connection) {
    int registrationStatus = -1; // -1 is a failure
    synchronized( registeredServers ) {
      synchronized( availableIdentifiers ) {
        if ( !availableIdentifiers.isEmpty() && !isRegistered( address ) ) {
          int identifier =
              availableIdentifiers.remove( availableIdentifiers.size()-1 );
          ServerConnection newConnection =
              new ServerConnection( identifier, address, connection );
          registeredServers.put( identifier, newConnection );
          registrationStatus = identifier; // registration successful
        }
      }
    }
    return registrationStatus;
  }

  /**
   * Removes the ChunkServer with a particular identifier from the
   * ServerConnectionCache. Since this ChunkServer may be storing essential
   * files for the operation of the distributed file system, files stored on the
   * ChunkServer must be relocated to other available ChunkServers.
   *
   * @param identifier of ChunkServer to deregister
   */
  public void deregister(int identifier) {
    // Remove from the registeredServers and availableIdentifiers
    // Remove all instances of identifier from each DistributedFileCache
    ArrayList<ServerFile> removedIdealStates;
    synchronized( registeredServers ) {
      synchronized( availableIdentifiers ) {
        ServerConnection connection = registeredServers.get( identifier );
        if ( connection == null ) { // no ChunkServer to remove
          return;
        }
        connection.getConnection().close(); // stop the receiver
        registeredServers.remove( identifier );
        availableIdentifiers.add( identifier ); // add back identifier

        // Must be sure that this is a safe operation
        removedIdealStates = idealState.removeAllFilesAtServer( identifier );
        reportedState.removeAllFilesAtServer( identifier );
      }
    }

    // Find the best candidates for relocation
    ArrayList<String> freestServers = listFreestServers();
    if ( freestServers.isEmpty() ) { // no servers left for relocation
      return;
    }

    // Iterate through displaced replicas and relocate them to freest
    // ChunkServers
    for ( ServerFile file : removedIdealStates ) {
      String storageInfo =
          idealState.getChunkStorageInfo( file.filename, file.sequence );
      String[] servers = file.getType() == Constants.CHUNK_TYPE ?
                             storageInfo.split( "\\|", -1 )[0].split( "," ) :
                             storageInfo.split( "\\|", -1 )[1].split( "," );

      if ( servers[0].isEmpty() ) {
        continue;
      }

      List<String> chunkServers = Arrays.asList( servers );
      for ( String freeServer : freestServers ) {
        if ( chunkServers.contains( freeServer ) ) {
          continue; // don't store two replicas on one ChunkServer
        }

        int freeServerIdentifier = Integer.parseInt( freeServer );
        String freeServerAddress =
            getChunkServerAddress( freeServerIdentifier );

        if ( freeServerAddress.isEmpty() ) {
          continue;
        }

        String filename;
        Event relocateFileMessage;
        String addressToContact;
        if ( file.getType() == Constants.CHUNK_TYPE ) { // It is a chunk
          filename = file.filename+"_chunk"+file.sequence;
          // Remove destination server from list of servers
          servers =
              ArrayUtilities.removeFromArray( servers, freeServerAddress );
          RepairChunk repairChunk =
              new RepairChunk( filename, freeServerAddress,
                  new int[]{ 0, 1, 2, 3, 4, 5, 6, 7 }, servers );
          addressToContact = repairChunk.getAddress();
          relocateFileMessage = repairChunk;
        } else { // It is a shard
          filename = file.filename+"_chunk"+file.sequence+"_shard"+
                     (( Shard ) file).fragment;
          // Replace "-1" with null in shard server array
          ArrayUtilities.replaceArrayItem( servers, "-1", null );
          RepairShard repairShard =
              new RepairShard( filename, freeServerAddress, servers );
          addressToContact = repairShard.getAddress();
          relocateFileMessage = repairShard;
        }

        // Send message to addressToContact so they can forward the file
        try {
          getConnection( addressToContact ).getConnection()
                                           .getSender()
                                           .sendData(
                                               relocateFileMessage.getBytes() );
          // If message is sent, add file with modified serverIdentifier to
          // idealState
          file.serverIdentifier = freeServerIdentifier;
          if ( file.getType() == Constants.CHUNK_TYPE ) {
            idealState.addChunk( ( Chunk ) file );
          } else {
            idealState.addShard( ( Shard ) file );
          }
          break;
        } catch ( IOException ioe ) {
          System.err.println( "deregister: Unable to send message to relocate"+
                              " a file from a deregistered ChunkServer. "+
                              ioe.getMessage() );
        }
      }
    }
  }

  /**
   * Broadcast a message to all registered ChunkServers.
   *
   * @param marshalledBytes message to send
   */
  public void broadcast(byte[] marshalledBytes) {
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        try {
          connection.getConnection().getSender().sendData( marshalledBytes );
        } catch ( IOException ioe ) {
          System.err.println(
              "broadcast: Unable to send message to ChunkServer "+
              connection.getIdentifier()+". "+ioe.getMessage() );
        }
      }
    }
  }

  /**
   * Class to house Comparators which will be used in the methods of the
   * ServerConnectionCache.
   */
  public static class Comparators {

    // Will be provided a Long[] filled with Long[] tuples. Each tuple
    // is formatted [ FREE_SPACE, SERVER_ID ]. We want to sort based on
    // FREE_SPACE first, then by ID.
    public static Comparator<Long[]> SERVER_SORT = new Comparator<Long[]>() {
      @Override
      public int compare(Long[] l1, Long[] l2) {
        int compareSpace = l1[0].compareTo( l2[0] );
        if ( compareSpace == 0 ) {
          return l1[1].compareTo( l2[1] );
        }
        return compareSpace;
      }
    };

  }
}