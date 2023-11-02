package cs555.overlay.transport;

import cs555.overlay.util.ApplicationProperties;
import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.Constants;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RepairShard;

import java.io.IOException;
import java.util.*;

/**
 * Class to hold important information for the Controller. Contains a map of
 * registered ChunkServers, and a map (fileTable) containing information about
 * where files of a particular sequence number are stored on the DFS.
 *
 * @author hayne
 */
public class ControllerInformation {

  private final ArrayList<Integer> availableIdentifiers;
  private final Map<Integer, ServerConnection> registeredServers;

  // filename maps to an array list, where indices represent sequence numbers
  // elements of the arraylist are arrays of host:port strings whose indices,
  // in the case of erasure coding, represent the fragment number
  private final Map<String, TreeMap<Integer, String[]>> fileTable;

  private static final Comparator<ServerConnection> serverComparator =
      Comparator.comparingInt( ServerConnection::getUnhealthy )
                .thenComparing( ServerConnection::getTotalChunks )
                .thenComparing( ServerConnection::getFreeSpace,
                    Comparator.reverseOrder() );

  /**
   * Constructor. Creates a list of available identifiers, and HashMaps for both
   * the files and the connections to the ChunkServers.
   */
  public ControllerInformation() {
    this.registeredServers = new HashMap<Integer, ServerConnection>();
    this.availableIdentifiers = new ArrayList<Integer>();
    for ( int i = 1; i <= 32; ++i ) {
      this.availableIdentifiers.add( i );
    }

    this.fileTable = new HashMap<>();
  }

  /**
   * Getter for fileTable.
   *
   * @return fileTable
   */
  public Map<String, TreeMap<Integer, String[]>> getFileTable() {
    return fileTable;
  }

  /**
   * Getter for registeredServers.
   *
   * @return registeredServers
   */
  public Map<Integer, ServerConnection> getRegisteredServers() {
    return registeredServers;
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
      return null;
    }
  }

  /**
   * Returns an array containing all host:port addresses of registered servers.
   *
   * @return String[] of host:port addresses
   */
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

  /**
   * Returns the identifier of a server with a particular host:port address.
   *
   * @param address host:port string
   * @return identifier of server reachable at that address, -1 if there is no
   * registered server with that address
   */
  public int getChunkServerIdentifier(String address) {
    synchronized( registeredServers ) {
      for ( ServerConnection connection : registeredServers.values() ) {
        if ( connection.getServerAddress().equals( address ) ) {
          return connection.getIdentifier();
        }
      }
    }
    return -1; // can't return null
  }

  /**
   * Returns the host:port address of a server with a particular identifier.
   *
   * @param identifier of a server
   * @return the host:port address of that server, returns null if there is no
   * registered server with that identifier
   */
  public String getChunkServerAddress(int identifier) {
    synchronized( registeredServers ) {
      ServerConnection connection = registeredServers.get( identifier );
      if ( connection != null ) {
        return connection.getServerAddress();
      }
    }
    return null; // should return null, not empty string
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
    if ( fileTable.containsKey( filename ) ) {
      TreeMap<Integer, String[]> fileMap = fileTable.get( filename );
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
    if ( fileTable.containsKey( filename ) ) {
      TreeMap<Integer, String[]> fileMap = fileTable.get( filename );
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
    if ( fileTable.containsKey( filename ) ) {
      TreeMap<Integer, String[]> fileMap = fileTable.get( filename );
      if ( fileMap.containsKey( sequence ) ) {
        fileMap.get( sequence )[fragment] = address;
      }
    }
  }

  /**
   * Deletes an entire file from the table. Will be called from a synchronized
   * function in the Controller, and will be followed by an operation to delete
   * all filename entries from every ServerConnection too . Then, a broadcast
   * message will be sent out to all ServerConnections to delete the filename
   * from their storage.
   *
   * @param filename filename to be deleted
   */
  public void deleteFile(String filename) {
    fileTable.remove( filename );
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
    if ( fileTable.containsKey( filename ) ) {
      return fileTable.get( filename ).get( sequence );
    }
    return null;
  }

  /**
   * Returns a list of the host:port server addresses of all registered
   * ChunkServers, sorted by ascending totalChunks, then descending freeSpace.
   *
   * @return ArrayList<String> of host:port combinations of registered
   * ChunkServers
   */
  private ArrayList<String> listSortedServers() {
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
  // located in the ControllerInformation

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
    ArrayList<String> sortedServers = listSortedServers();

    int serversNeeded = 3; // standard replication factor
    if ( ApplicationProperties.storageType.equals( "erasure" ) ) {
      serversNeeded = Constants.TOTAL_SHARDS;
    }

    if ( sortedServers.size() >= serversNeeded ) { // are enough servers
      TreeMap<Integer, String[]> servers = fileTable.get( filename );
      if ( servers == null ) { // case of first chunk
        fileTable.put( filename, new TreeMap<>() );
        servers = fileTable.get( filename );
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
   * @param address host:port address of possible server
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
   * ControllerInformation. Since this ChunkServer may be storing essential
   * files for the operation of the distributed file system, files stored on the
   * ChunkServer must be relocated to other available ChunkServers. This
   * function should only be called if the caller holds an intrinsic lock on the
   * Controller, otherwise, there could be collisions of behavior.
   *
   * @param identifier of ChunkServer to deregister
   */
  public void deregister(int identifier) {
    // Remove from the registeredServers and availableIdentifiers
    // Remove all instances of its host:port from the fileTable
    // Find replacement servers for all of its files
    String deregisterAddress;
    Map<String, ArrayList<Integer>> storedChunks;
    synchronized( registeredServers ) {
      synchronized( availableIdentifiers ) {
        ServerConnection connection = registeredServers.get( identifier );
        if ( connection == null ) { // server has already been removed
          return;
        }
        connection.getConnection().close(); // stop the receiver
        registeredServers.remove( identifier );
        availableIdentifiers.add( identifier ); // add back identifier
        deregisterAddress = connection.getServerAddress();
        storedChunks = connection.getStoredChunks();
      }
    }

    // Find the best candidates for relocation
    ArrayList<String> sortedServers = listSortedServers(); // could be empty

    // Iterate through displaced replicas and relocate them to the servers
    // that are the best candidates
    for ( String filename : storedChunks.keySet() ) {
      for ( int sequence : storedChunks.get( filename ) ) {
        String[] servers = getServers( filename, sequence );
        boolean replaced = false;
        for ( String candidate : sortedServers ) {
          if ( ArrayUtilities.contains( servers, candidate ) == -1 ) {
            ArrayUtilities.replaceFirst( servers, deregisterAddress,
                candidate );
            ServerConnection replacement = getConnection( candidate );
            replacement.addChunk( filename, sequence );
            // Send replacement server the file it should now have
            boolean sent =
                sendReplacementMessage( filename, sequence, candidate,
                    servers );
            if ( !sent ) {
              ArrayUtilities.replaceFirst( servers, candidate,
                  deregisterAddress );
              replacement.removeChunk( filename, sequence );
              continue;
            }
            replaced = true;
            break;
          }
        }
        if ( !replaced ) { // set host:port to null in servers
          ArrayUtilities.replaceFirst( servers, deregisterAddress, null );
        }
      }
    }
  }

  /**
   * Constructs a message that will hopefully relocate a file from a server that
   * has deregistered to a server that has been selected as its replacement.
   * Slightly long because it must first construct the right type of message --
   * either RepairShard or RepairChunk, and then send the message to the correct
   * server.
   *
   * @param filename base filename of file to be relocated
   * @param sequence sequence number of chunk
   * @param destination host:port of the server the file should be relocated to
   * @param servers String[] of host:port addresses that replicas/fragments of
   * this particular file are located at
   * @return true if message is sent, false if it wasn't
   */
  private boolean sendReplacementMessage(String filename, int sequence,
      String destination, String[] servers) {
    String appendedFilename = filename+"_chunk"+sequence;
    String addressToContact;
    Event relocateMessage;
    if ( ApplicationProperties.storageType.equals( "replication" ) ) {
      // Remove destination server from list of servers
      servers = ArrayUtilities.removeFromArray( servers, destination );
      RepairChunk repairChunk = new RepairChunk( appendedFilename, destination,
          new int[]{ 0, 1, 2, 3, 4, 5, 6, 7 }, servers );
      addressToContact = repairChunk.getAddress();
      relocateMessage = repairChunk;
    } else { // erasure coding
      int fragment = ArrayUtilities.contains( servers, destination );
      appendedFilename = appendedFilename+"_shard"+fragment;
      System.out.println( appendedFilename );
      RepairShard repairShard =
          new RepairShard( appendedFilename, destination, servers );
      addressToContact = repairShard.getAddress();
      relocateMessage = repairShard;
    }
    try {
      getConnection( addressToContact ).getConnection()
                                       .getSender()
                                       .sendData( relocateMessage.getBytes() );
    } catch ( IOException ioe ) {
      System.err.println(
          "sendReplacementMessage: There was an error sending a message to "+
          addressToContact+" to replace '"+appendedFilename+"' at "+destination+
          ". "+ioe.getMessage() );
      return false;
    }
    return true;
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