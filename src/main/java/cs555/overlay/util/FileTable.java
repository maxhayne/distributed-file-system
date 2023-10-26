package cs555.overlay.util;

import cs555.overlay.transport.ServerConnectionCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class FileTable {
  // filename maps to an array list, where indices represent sequence numbers
  // elements of the arraylist are arrays of host:port strings whose indices,
  // in the case of erasure coding, represent the fragment number
  private final Map<String, TreeMap<Integer, String[]>> table;
  private final ServerConnectionCache registeredServers;

  public FileTable(ServerConnectionCache registeredServers) {
    this.table = new HashMap<>();
    this.registeredServers = registeredServers;
  }

  // this cannot work properly unless servers cannot register or deregister
  // while this function is operating, how can we achieve this?
  // could keep a hashtable in each chunkserverconnection that holds the
  // files stored at that chunkserver, but then we'd need to iterate over all
  // servers to get the storage info of a particular chunk, which is
  // inefficient. but if the chunkserverconnectioncache were sorted, we could
  // iterate through chunkserverconnections to add instances of the chunk
  // being stored. no, that's bad. that would avoid iterating over a list
  // to find the particular chunks stored at a particular server, but other
  // than that it would be inefficient.
  // We could use a parent class to store both the data structure used for
  // keeping track of chunk locations and the data structure used to keep
  // track of registered servers, and then ensure that this function requires
  // double synchronization, but that is also messy, and could be hard to get
  // right.
  // the best way to achieve it is to synchronize the register and deregister
  // functions inside the Controller, and to synchronize the message case
  // that calls this function too. This means that the HeartbeatMonitor must
  // call the Controller.deregister function, not the deregister function
  // located in the ServerConnectionCache

  // doesn't need local synchronization, will only be called via synchronized
  // methods in the Controller (register and deregister will also be
  // synchronized)

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
    ArrayList<String> sortedServers = registeredServers.sortServers();

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

  // this will be called via a synchronized method in the Controller as well,
  // when the Client is requesting storage information

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

  // this will be called with synchronized method from Controller after
  // receiving a ChunkServerReportsFileCorruption Message

  /**
   * Removes (sets to null) a particular host:port address of a server from a
   * chunk's list of servers. Is used when the Controller receives a message
   * that the replication/fragment located at that server is corrupt.
   *
   * @param filename base filename of chunk -- what comes before "_chunk"
   * @param sequence the sequence number of the chunk
   * @param address the address to remove from the chunk's servers
   */
  public void removeServer(String filename, int sequence, String address) {
    if ( table.containsKey( filename ) ) {
      TreeMap<Integer, String[]> fileMap = table.get( filename );
      if ( fileMap.containsKey( sequence ) ) {
        ArrayUtilities.replaceArrayItem( fileMap.get( sequence ), address,
            null );
      }
    }
  }

  // these next two functions will also be called via synchronized methods in
  // the Controller via ChunkServerReportsFileFixed messages, which will
  // contain the filename, and thus will inform on which function to use

  /**
   * Adds a server's host:port address to the list of servers storing a
   * particular chunk. Will be called when a server reports that it has repaired
   * a chunk that was previously corrupt.
   *
   * @param filename base filename of chunk -- what comes before "_chunk"
   * @param sequence the sequence number of the chunk
   * @param address the address to add to the chunk's servers
   */
  public void addServer(String filename, int sequence, String address) {
    if ( table.containsKey( filename ) ) {
      TreeMap<Integer, String[]> fileMap = table.get( filename );
      if ( fileMap.containsKey( sequence ) ) {
        ArrayUtilities.replaceFirst( fileMap.get( sequence ), null, address );
      }
    }
  }

  /**
   * Adds a server's host:port address to the list of servers storing a
   * particular chunk, at a particular fragment number. Will be called when a
   * server reports that it has fixed a shard that was previously corrupt.
   *
   * @param filename base filename of chunk -- what comes before "_chunk"
   * @param sequence the sequence number of the chunk
   * @param fragment the fragment number of the chunk
   * @param address the address to add to the chunk's servers
   */
  public void addServer(String filename, int sequence, int fragment,
      String address) {
    if ( table.containsKey( filename ) ) {
      TreeMap<Integer, String[]> fileMap = table.get( filename );
      if ( fileMap.containsKey( sequence ) ) {
        fileMap.get( sequence )[fragment] = address;
      }
    }
  }

  /**
   * Deletes an entire file from the table. Will be called from a
   * synchronized function in the Controller, and will be followed by an
   * operation to delete all filename entries from every ServerConnection too
   * . Then, a broadcast message will be sent out to all ServerConnections to
   * delete the filename from their storage.
   *
   * @param filename filename to be deleted
   */
  public void deleteFile(String filename) {
    table.remove( filename );
  }

}
