package cs555.overlay.transport;

import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.Constants;
import cs555.overlay.util.HeartbeatMonitor;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RepairShard;

import java.io.IOException;
import java.util.*;

public class ServerConnectionCache {

  private final ArrayList<Integer> availableIdentifiers;
  private final Map<Integer, ServerConnection> registeredServers;

  private final Map<String, TreeMap<Integer, String[]>> table;

  private static final Comparator<ServerConnection> serverComparator =
      Comparator.comparingInt( ServerConnection::getTotalChunks )
                .thenComparing( ServerConnection::getFreeSpace,
                    Comparator.reverseOrder() );

  public ServerConnectionCache() {
    this.table = new HashMap<>();
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
   * Returns the ServerConnection object of a registered ChunkServer with the
   * identifier specified as a parameter.
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
   * Return the ServerConnection object of a registered ChunkServer with the
   * host:port address specified as a parameter.
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

  /**
   * Returns the set of servers storing the particular chunk with that filename
   * and sequence number.
   *
   * @param filename base filename of chunk -- what comes before "_chunk#"
   * @param sequence the sequence number of the chunk
   * @return String[] of host:port addresses to the servers storing this
   * particular chunk
   */
  public String[] getServers(String filename, int sequence) {
    if ( table.containsKey( filename ) ) {
      return table.get( filename ).get( sequence );
    }
    return null;
  }

  private ArrayList<String> getSortedServers() {
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

  /**
   * Allocates a set of servers to store a particular chunk of a particular
   * file. If the particular chunk was previously allocated a set of servers, it
   * overwrites the previous allocation.
   *
   * @param filename base filename of chunk -- what comes before "_chunk#"
   * @param sequence the sequence number of the chunk
   * @return String[] of host:port addresses to the servers which should store
   * this chunk, or null, if servers couldn't be allocated
   */
  public String[] allocateServers(String filename, int sequence) {
    ArrayList<String> sortedServers = getSortedServers();

    int serversNeeded = 3; // standard replication factor
    if ( ApplicationProperties.storageType.equals( "erasure" ) ) {
      serversNeeded = Constants.TOTAL_SHARDS;
    }

    if ( sortedServers.size() >= serversNeeded ) { // are enough servers
      TreeMap<Integer, String[]> servers = table.get( filename );
      if ( servers == null ) { // case of first chunk
        table.put( filename, new TreeMap<>() );
        servers = table.get( filename );
      }
      String[] reservedServers = new String[serversNeeded];
      for ( int i = 0; i < serversNeeded; ++i ) {
        reservedServers[i] = sortedServers.get( i );
      }
      servers.put( sequence, reservedServers );
      // Need to add the file to the ServerConnection instance, in a
      // data structure that holds the list of files that should be stored at
      // that ChunkServer
      return reservedServers;
    }
    return null;
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
}