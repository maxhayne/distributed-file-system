package cs555.overlay.transport;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.config.Constants;
import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.ForwardInformation;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.GeneralMessage;
import cs555.overlay.wireformats.Protocol;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RepairShard;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class to hold important information for the Controller. Contains a map of
 * registered ChunkServers, and a map (fileTable) containing information about
 * where files of a particular sequence number are stored on the DFS.
 *
 * @author hayne
 */
public class ControllerInformation {

  private static final Logger logger = Logger.getInstance();
  private final Queue<Integer> idPool;
  private final Map<Integer,ServerConnection> servers;

  // Filename maps to a TreeMap where keys are sequence numbers.
  // Sequence numbers then map to arrays containing host:port addresses of
  // the servers storing that particular chunk.
  private final Map<String,TreeMap<Integer,String[]>> fileTable;

  private static final Comparator<ServerConnection> serverComparator =
      Comparator.comparingInt(ServerConnection::getUnhealthy)
                .thenComparing(ServerConnection::totalStoredChunks)
                .thenComparing(ServerConnection::getFreeSpace,
                    Comparator.reverseOrder());

  /**
   * Constructor. Creates a list of available identifiers, and HashMaps for both
   * the files and the connections to the ChunkServers.
   */
  public ControllerInformation() {
    this.servers = new HashMap<>();
    this.fileTable = new HashMap<>();
    this.idPool = new LinkedBlockingQueue<>();
    for (int i = 1; i <= 32; ++i) {
      this.idPool.add(i);
    }
  }

  /**
   * Getter for fileTable.
   *
   * @return fileTable
   */
  public Map<String,TreeMap<Integer,String[]>> getFileTable() {
    return fileTable;
  }

  public List<String> fileList() {
    synchronized(fileTable) {
      return fileTable.keySet().stream().toList();
    }
  }

  public String[][] getFileStorageDetails(String filename) {
    String[][] servers = null;
    synchronized(fileTable) {
      if (fileTable.containsKey(filename)) {
        TreeMap<Integer,String[]> chunks = fileTable.get(filename);
        servers = new String[chunks.size()][];
        int index = 0;
        for (String[] chunkServer : chunks.values()) {
          servers[index] = chunkServer.clone(); // TODO is this necessary?
          index++;
        }
      }
    }
    return servers;
  }

  /**
   * Getter for registeredServers.
   *
   * @return registeredServers
   */
  public Map<Integer,ServerConnection> getRegisteredServers() {
    return servers;
  }

  public List<String> serverDetailsList() {
    synchronized(servers) {
      return servers.values().stream().map(ServerConnection::toString).toList();
    }
  }

  /**
   * Returns the ServerConnection object of a registered ChunkServer with the
   * identifier specified as a parameter.
   *
   * @param identifier of ChunkServer
   * @return ServerConnection with that identifier, null if doesn't exist
   */
  public ServerConnection getConnection(int identifier) {
    synchronized(servers) {
      return servers.get(identifier);
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
    logger.debug("About to acquire lock on 'servers'");
    synchronized(servers) {
      logger.debug("Acquired lock on 'servers'");
      for (ServerConnection connection : servers.values()) {
        if (connection.getServerAddress().equals(address)) {
          return connection;
        }
      }
    }
    return null;
  }

  /**
   * Returns the host:port address of a server with a particular identifier.
   *
   * @param identifier of a server
   * @return the host:port address of that server, returns null if there is no
   * registered server with that identifier
   */
  public String getChunkServerAddress(int identifier) {
    ServerConnection connection;
    synchronized(servers) {
      connection = servers.get(identifier);
    }
    return connection != null ? connection.getServerAddress() : null;
  }

  /**
   * Attempts to fully delete a file from the DFS. First, deletes the file from
   * the fileTable, then removes the file from all ServerConnection storedChunks
   * maps, then sends out broadcast message to delete the file at all servers.
   *
   * @param filename filename to be deleted
   */
  public void deleteFileFromDFS(String filename) {
    synchronized(servers) {
      synchronized(fileTable) {
        fileTable.remove(filename);
        // TODO remove the step from server.deleteFile that attempts to
        //  remove entries with that filename from missingChunks, which will,
        //  if the HeartbeatMonitor is in progress, produce deadlock
        servers.values().forEach(server -> server.deleteFile(filename));
        // Send message to all servers to delete
        GeneralMessage deleteRequest =
            new GeneralMessage(Protocol.CONTROLLER_REQUESTS_FILE_DELETE,
                filename);
        try {
          broadcast(deleteRequest.getBytes());
        } catch (IOException ioe) {
          logger.debug(
              "Problem sending file delete request to all ChunkServers. " +
              ioe.getMessage());
        }
      }
    }
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
    synchronized(fileTable) {
      if (fileTable.containsKey(filename)) {
        return fileTable.get(filename).get(sequence);
      }
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
  private List<String> sortedServers() {
    synchronized(servers) {
      List<ServerConnection> sortedServers = new ArrayList<>(servers.values());
      sortedServers.sort(serverComparator);
      return sortedServers.stream()
                          .map(ServerConnection::getServerAddress)
                          .toList();
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
    String[] reservedServers = null;
    int serversNeeded = ApplicationProperties.storageType.equals("erasure") ?
                            Constants.TOTAL_SHARDS : 3;
    synchronized(servers) {
      List<String> sortedServers = sortedServers(); // reentrant
      if (sortedServers.size() >= serversNeeded) { // are enough servers
        synchronized(fileTable) {
          fileTable.putIfAbsent(filename, new TreeMap<>());
          TreeMap<Integer,String[]> fileServers = fileTable.get(filename);
          reservedServers = new String[serversNeeded];
          for (int i = 0; i < serversNeeded; ++i) {
            reservedServers[i] = sortedServers.get(i);
          }
          fileServers.put(sequence, reservedServers);
          // TODO figure out if this operation needs fileTable synchronization
          // add chunk to reservedServer's storedChunks
          for (String reservedServer : reservedServers) {
            getConnection(reservedServer).addChunk(filename, sequence);
          }
        }
      }
    }
    return reservedServers;
  }

  /**
   * Returns whether particular host:port address has registered as a
   * ChunkServer.
   *
   * @param address host:port address of possible server
   * @return true if registered, false if not
   */
  public boolean isRegistered(String address) {
    synchronized(servers) {
      for (ServerConnection connection : servers.values()) {
        if (address.equals(connection.getServerAddress())) {
          return true;
        }
      }
    }
    return false;
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
    synchronized(servers) {
      if (!isRegistered(address)) {
        Integer identifier = idPool.poll();
        if (identifier != null) {
          ServerConnection newServer =
              new ServerConnection(identifier, address, connection);
          servers.put(identifier, newServer);
          assignUnderReplicatedChunks(newServer); // assign chunks to server
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
   * @param server newly registered server
   */
  private void assignUnderReplicatedChunks(ServerConnection server) {
    String address = server.getServerAddress();
    synchronized(fileTable) {
      for (String filename : fileTable.keySet()) {
        for (Map.Entry<Integer,String[]> entry : fileTable.get(filename)
                                                          .entrySet()) {
          String[] servers = entry.getValue();
          int nullIndex = ArrayUtilities.contains(servers, null);
          if (nullIndex != -1) {
            servers[nullIndex] = address;
            server.addChunk(filename, entry.getKey());
            logger.debug(
                "Assigned " + filename + "_chunk" + entry.getKey() + " " +
                nullIndex + " to " + address);
          }
        }
      }
    }
  }

  /**
   * Deregisters ChunkServers from ControllerInformation. Since they might be
   * storing essential files for the operation of the system, files that were
   * stored on the deregistering ChunkServers must be relocated to other active
   * ChunkServers, if possible. The reason the function takes a list of
   * identifiers is to ensure that if the HeartbeatMonitor detects multiple
   * failures, we deal with them together, which saves us from trying to
   * relocate files to servers that we know have failed, or from thinking that
   * files are recoverable when they aren't.
   *
   * @param identifiers list of ChunkServers to deregister
   */
  public void deregister(List<Integer> identifiers) {
    synchronized(servers) {
      List<ServerConnection> removedServers = removeServersAndGet(identifiers);
      synchronized(fileTable) {
        removeServersFromTable(removedServers);
        removeUnrecoverableChunks();
        deleteUnrecoverableFiles();
        repairUnderReplicatedChunks();
      }
    }
  }

  private void removeUnrecoverableChunks() {
    synchronized(fileTable) {
      fileTable.forEach((filename, chunkMap) -> {
        Iterator<Map.Entry<Integer,String[]>> it =
            chunkMap.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Integer,String[]> entry = it.next();
          Integer sequence = entry.getKey();
          String[] servers = entry.getValue();
          if (!isChunkRecoverable(servers)) {
            // Remove chunk from storedChunks of servers
            for (String server : servers) {
              if (server != null) {
                ServerConnection connection = getConnection(server);
                if (connection != null) {
                  connection.removeChunk(filename, sequence);
                }
              }
            }
            // Remove chunk from the fileTable
            it.remove();
          }
        }
      });
    }
  }

  private void repairUnderReplicatedChunks() {
    synchronized(servers) {
      synchronized(fileTable) {
        fileTable.forEach((filename, chunkMap) -> {
          chunkMap.forEach((sequence, serverArray) -> {
            if (isChunkRecoverable(serverArray)) {
              List<String> bestCandidates = sortedServers();
              repairChunk(filename, sequence, serverArray, bestCandidates);
            }
          });
        });
      }
    }
  }

  private List<ServerConnection> removeServersAndGet(List<Integer> ids) {
    List<ServerConnection> removedServers = new ArrayList<>();
    synchronized(servers) {
      for (int id : ids) {
        ServerConnection server = servers.get(id);
        if (server != null) { // hasn't already been deregistered
          server.getConnection().close(); // stop the receiver
          servers.remove(id);
          idPool.add(id); // add id back to pool
          removedServers.add(server);
        }
      }
    }
    return removedServers;
  }

  private void removeServersFromTable(List<ServerConnection> servers) {
    synchronized(fileTable) {
      for (ServerConnection server : servers) {
        String address = server.getServerAddress();
        fileTable.forEach((filename, chunkMap) -> {
          chunkMap.forEach((sequence, serverArray) -> {
            ArrayUtilities.replaceFirst(serverArray, address, null);
          });
        });
      }
    }
  }

  private void repairChunk(String filename, int sequence, String[] servers,
      List<String> candidates) {
    for (int i = 0; i < servers.length; ++i) {
      if (servers[i] == null) {
        for (String candidate : candidates) {
          if (ArrayUtilities.contains(servers, candidate) == -1) {
            ServerConnection server = getConnection(candidate);
            servers[i] = candidate;
            if (sendReplacementMessage(filename, sequence, candidate,
                servers)) {
              logger.debug("About to add " + filename + "_chunk" + sequence +
                           " to the storedChunks of " + candidate);
              server.addChunk(filename, sequence); // TODO getting locked here?
              logger.debug(
                  "Moving " + filename + "_chunk" + sequence + " " + i +
                  " to " + candidate);
              break;
            } else {
              servers[i] = null;
            }
          }
        }
      }
    }
  }

  private static boolean isChunkRecoverable(String[] servers) {
    if (servers != null) {
      int nullCount = ArrayUtilities.countNulls(servers);
      if (ApplicationProperties.storageType.equals("erasure")) {
        return nullCount <= Constants.TOTAL_SHARDS - Constants.DATA_SHARDS;
      } else {
        return nullCount < 3;
      }
    } else {
      return false;
    }
  }

  /**
   * Calls deleteFileFromDFS for any file that has no recoverable chunks.
   */
  private void deleteUnrecoverableFiles() {
    List<String> deletedFiles = new ArrayList<>();
    synchronized(servers) {
      synchronized(fileTable) {
        fileTable.entrySet().removeIf(entry -> {
          if (entry.getValue().isEmpty()) {
            deletedFiles.add(entry.getKey());
            logger.debug(entry.getKey() + " is unrecoverable. Deleting.");
            return true;
          }
          return false;
        });
        deletedFiles.forEach(this::deleteFileFromDFS);
      }
    }
  }

  /**
   * Dispatch repair/replace messages for all files in a ChunkServer's
   * storedChunks map.
   *
   * @param identifier of ChunkServer to refresh files for
   */
  public void refreshServerFiles(int identifier) {
    synchronized(servers) {
      synchronized(fileTable) {
        ServerConnection connection = servers.get(identifier);
        if (connection != null) {
          // TODO figure this out. The getStoredChunks() is called here and
          //  in the HeartbeatMonitor. This means that if there is nothing
          //  preventing this function and the monitor running at the same time,
          //  we could be iterating over it at the same time. We won't be
          //  modifying it concurrently, though. This requires more thought.
          String address = connection.getServerAddress();
          connection.getStoredChunks().forEach((filename, sequences) -> {
            sequences.forEach((sequence) -> {
              String[] servers = fileTable.get(filename).get(sequence);
              boolean sent =
                  sendReplacementMessage(filename, sequence, address, servers);
              String NOT = sent ? "" : "NOT ";
              logger.debug(
                  filename + "_chunk" + sequence + " was " + NOT + "sent to " +
                  address);
            });
          });
        }
      }
    }
  }

  /**
   * Constructs a message that will try to relocate a file from a server that
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
    String specificFilename = filename + "_chunk";
    specificFilename = ApplicationProperties.storageType.equals("erasure") ?
                           specificFilename + sequence + "_shard" +
                           ArrayUtilities.contains(servers, destination) :
                           specificFilename + sequence;

    ForwardInformation forwardInformation =
        constructRepairMessage(specificFilename, servers, destination,
            new int[]{0, 1, 2, 3, 4, 5, 6, 7});

    if (forwardInformation.firstHop() != null) {
      try {
        String address = forwardInformation.firstHop();
        logger.debug("About to send replacement message to " + address);
        getConnection(address).getConnection()
                              .getSender()
                              .sendData(forwardInformation.repairMessage()
                                                          .getBytes());
        logger.debug("Sent replacement message to " + address);
        return true;
      } catch (IOException ioe) {
        logger.debug(
            "Unable to send a message to " + forwardInformation.firstHop() +
            " to replace '" + specificFilename + "' at " + destination + ". " +
            ioe.getMessage());
      }
    }
    return false;
  }

  /**
   * Broadcast a message to all registered ChunkServers.
   *
   * @param marshalledBytes message to send
   */
  public void broadcast(byte[] marshalledBytes) {
    synchronized(servers) {
      for (ServerConnection connection : servers.values()) {
        try {
          connection.getConnection().getSender().sendData(marshalledBytes);
        } catch (IOException ioe) {
          logger.debug("Unable to send message to ChunkServer " +
                       connection.getIdentifier() + ". " + ioe.getMessage());
        }
      }
    }
  }

  /**
   * Constructs a repair message of the correct type if there are enough servers
   * that might hold the file in the servers array.
   *
   * @param filename of file to be repaired
   * @param servers array of servers that hold the file
   * @param destination address of server that needs the repair
   * @param slices specific slices that need repairing, is ignored if using
   * erasure coding
   * @return a ForwardInformation object containing the address of the first
   * server to forward the message to, and the actual message to be sent, or a
   * ForwardInformation object with null values, if message couldn't be
   * constructed
   */
  public static ForwardInformation constructRepairMessage(String filename,
      String[] servers, String destination, int[] slices) {
    if (isChunkRecoverable(servers)) {
      if (ApplicationProperties.storageType.equals("erasure")) {
        RepairShard repairShard =
            new RepairShard(filename, destination, servers);
        return new ForwardInformation(repairShard.getAddress(), repairShard);
      } else {
        String[] strippedServers =
            ArrayUtilities.reduceReplicationServers(servers, destination);
        if (strippedServers.length != 0) {
          RepairChunk repairChunk =
              new RepairChunk(filename, destination, slices, strippedServers);
          return new ForwardInformation(repairChunk.getAddress(), repairChunk);
        }
      }
    }
    return new ForwardInformation(null, null);
  }
}