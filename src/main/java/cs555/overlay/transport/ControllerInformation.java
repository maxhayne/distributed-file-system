package cs555.overlay.transport;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.*;

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

  private static final Logger logger = Logger.getInstance();
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
    this.registeredServers = new HashMap<>();
    this.availableIdentifiers = new ArrayList<>();
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

  /**
   * Attempts to fully delete a file from the DFS. First, deletes the file from
   * the fileTable, then removes the file from all ServerConnection storedChunks
   * maps, then sends out broadcast message to delete the file at all servers.
   *
   * @param filename filename to be deleted
   */
  public void deleteFileFromDFS(String filename) {
    fileTable.remove( filename );
    // Delete from the storedChunks map of all servers in registeredServers
    for ( ServerConnection server : registeredServers.values() ) {
      server.deleteFile( filename );
    }
    // Send message to all servers to delete
    GeneralMessage deleteRequest =
        new GeneralMessage( Protocol.CONTROLLER_REQUESTS_FILE_DELETE,
            filename );
    try {
      broadcast( deleteRequest.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug( "Problem sending file delete request to all ChunkServers. "+
                    ioe.getMessage() );
    }
  }

  // This will be called via a synchronized method in the Controller as well,
  // when the Client is requesting storage information.

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

  // Doesn't need local synchronization, will only be called via synchronized
  // methods in the Controller (register and deregister will also be
  // synchronized).

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
   * registration fails, returns -1, else it returns the identifier. If the
   * registration is successful, the newly registered server is assigned chunks
   * that were previously not held by anyone (in an attempt to bring files back
   * up to their proper replication factor).
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
          assignNullReplications( newConnection ); // assign server chunks
          registrationStatus = identifier; // registration successful
        }
      }
    }
    return registrationStatus;
  }

  /**
   * Iterates through the fileTable and assigns one null value per chunk to be
   * taken up by the newly registered connection. (a null value meaning that the
   * chunk isn't being stored by anyone right now, but it should be)
   *
   * @param connection newly registered connection
   */
  private void assignNullReplications(ServerConnection connection) {
    synchronized( fileTable ) {
      for ( String filename : fileTable.keySet() ) {
        for ( Map.Entry<Integer, String[]> entry : fileTable.get( filename )
                                                            .entrySet() ) {
          String[] servers = entry.getValue();
          int nullIndex = ArrayUtilities.contains( servers, null );
          if ( nullIndex != -1 ) {
            servers[nullIndex] = connection.getServerAddress();
            connection.addChunk( filename, entry.getKey() );
            logger.debug( "Assigned "+filename+"_chunk"+entry.getKey()+" to "+
                          connection.getServerAddress() );
          }
        }
      }
    }
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
            logger.debug(
                "Moving "+filename+"_chunk"+sequence+" to "+candidate );
            replaced = true;
            break;
          }
        }
        if ( !replaced ) { // set host:port to null in servers
          ArrayUtilities.replaceFirst( servers, deregisterAddress, null );
          deleteChunkIfUnrecoverable( servers, filename, sequence );
        }
      }
    }
    // Deletes all files that have no more recoverable chunks
    deleteUnrecoverableFiles();
  }

  /**
   * Checks if a chunk cannot be recovered and deletes it if it can't. If
   * replicating, that means that a chunk no longer has any replicas. If erasure
   * coding, that means that fewer than the needed number of fragments to
   * recover the chunk are available.
   *
   * @param servers String[] servers supposedly storing the chunk
   * @param filename base filename of the chunk
   * @param sequence sequence number of the chunk
   */
  private void deleteChunkIfUnrecoverable(String[] servers, String filename,
      int sequence) {
    int numberOfNulls = ArrayUtilities.countNulls( servers );
    if ( ApplicationProperties.storageType.equals( "erasure" ) ) {
      if ( numberOfNulls > Constants.TOTAL_SHARDS-Constants.DATA_SHARDS ) {
        fileTable.get( filename ).remove( sequence );
        logger.debug(
            "Deleting "+filename+"_chunk"+sequence+" from the fileTable." );
      }
    } else { // replication
      if ( numberOfNulls == 3 ) {
        fileTable.get( filename ).remove( sequence );
        logger.debug(
            "Deleting "+filename+"_chunk"+sequence+" from the fileTable." );
      }
    }
  }

  /**
   * Calls the deleteFileFromDFS() method on any file that no longer has any
   * valid recoverable chunks.
   */
  private void deleteUnrecoverableFiles() {
    for ( Map.Entry<String, TreeMap<Integer, String[]>> entry :
        fileTable.entrySet() ) {
      if ( entry.getValue().isEmpty() ) {
        deleteFileFromDFS( entry.getKey() );
        logger.debug( entry.getKey()+" is unrecoverable. Deleting." );
      }
    }
  }

  /**
   * Dispatch repair/replace messages for all files in a ChunkServer's
   * 'storedChunks' map.
   *
   * @param identifier of ChunkServer to refresh files for
   */
  public void refreshServerFiles(int identifier) {
    ServerConnection connection = registeredServers.get( identifier );
    for ( Map.Entry<String, ArrayList<Integer>> entry :
        connection.getStoredChunks()
                                                                  .entrySet() ) {
      for ( Integer sequence : entry.getValue() ) {
        boolean sent = sendReplacementMessage( entry.getKey(), sequence,
            connection.getServerAddress(),
            fileTable.get( entry.getKey() ).get( sequence ) );
        String not = sent ? "" : "NOT ";
        logger.debug( entry.getKey()+"_chunk"+sequence+" was "+not+"sent to "+
                      connection.getServerAddress() );
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
    if ( ApplicationProperties.storageType.equals( "erasure" ) ) { // erasure
      int fragment = ArrayUtilities.contains( servers, destination );
      appendedFilename = appendedFilename+"_shard"+fragment;
      RepairShard repairShard =
          new RepairShard( appendedFilename, destination, servers );
      addressToContact = repairShard.getAddress();
      relocateMessage = repairShard;
    } else { // replication
      // Remove destination server from list of servers
      servers = ArrayUtilities.removeFromArray( servers, destination );
      RepairChunk repairChunk = new RepairChunk( appendedFilename, destination,
          new int[]{ 0, 1, 2, 3, 4, 5, 6, 7 }, servers );
      addressToContact = repairChunk.getAddress();
      relocateMessage = repairChunk;
    }
    try {
      getConnection( addressToContact ).getConnection()
                                       .getSender()
                                       .sendData( relocateMessage.getBytes() );
    } catch ( IOException ioe ) {
      logger.debug(
          "Unable to send a message to "+addressToContact+" to replace '"+
          appendedFilename+"' at "+destination+". "+ioe.getMessage() );
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
          logger.debug( "Unable to send message to ChunkServer "+
                        connection.getIdentifier()+". "+ioe.getMessage() );
        }
      }
    }
  }
}