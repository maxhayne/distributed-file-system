package cs555.overlay.util;

import cs555.overlay.config.Constants;
import cs555.overlay.node.Controller;
import cs555.overlay.transport.ControllerInformation;
import cs555.overlay.transport.ServerConnection;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.wireformats.GeneralMessage;
import cs555.overlay.wireformats.Protocol;
import cs555.overlay.wireformats.RepairChunk;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Is run on a timer every Constants.HEARTRATE milliseconds. Reads the latest
 * heartbeat sent by each registered ChunkServer and prints out information to
 * the terminal. Detects whether a ChunkServer is failing and ought to be
 * deregistered. On major heartbeats, tries to replace files at servers that are
 * missing.
 *
 * @author hayne
 */
public class HeartbeatMonitor extends TimerTask {

  private static final Logger logger = Logger.getInstance();
  private final Controller controller;
  private final ControllerInformation information;
  private final TCPConnectionCache connectionCache;

  public HeartbeatMonitor(Controller controller,
      ControllerInformation information) {
    this.controller = controller;
    this.information = information;
    this.connectionCache = new TCPConnectionCache(controller);
  }

  /**
   * Creates a long String of filenames sent by the ChunkServer in the most
   * recent heartbeat.
   *
   * @param files ArrayList of FileMetadatas from latest heartbeat message
   * @return String of files
   */
  private String createStringOfFiles(ArrayList<FileMetadata> files) {
    if (!files.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (FileMetadata newFile : files) {
        sb.append(newFile.getFilename()).append(", ");
      }
      sb.delete(sb.length() - 2, sb.length());
      return "[ " + sb + " ]";
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
    if (info.getLastMinorHeartbeat() == 0 &&
        info.getLastMajorHeartbeat() == 0) {
      return -1;
    }
    return now - info.getLastMajorHeartbeat() <
           now - info.getLastMinorHeartbeat() ? 1 : 0;
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
    if (info.getLastMajorHeartbeat() != 0 &&
        now - info.getLastMajorHeartbeat() > (Constants.HEARTRATE*11)) {
      unhealthyScore++;
    }
    // ChunkServer has missed a minor heartbeat
    if (info.getLastMinorHeartbeat() != 0 &&
        now - info.getLastMinorHeartbeat() > (Constants.HEARTRATE*2)) {
      unhealthyScore += 1 + (int) (
          (now - info.getLastMinorHeartbeat() - (Constants.HEARTRATE*2))/
          Constants.HEARTRATE); // extra unhealthy for more
    }
    // Hasn't even sent a minor heartbeat
    if (now - connectionStartTime > (Constants.HEARTRATE*2) &&
        info.getLastMinorHeartbeat() == 0) {
      unhealthyScore += 1;
    } // Hasn't even sent a major heartbeat
    if (now - connectionStartTime > Constants.HEARTRATE &&
        info.getLastMajorHeartbeat() == 0) {
      unhealthyScore += 1;
    }
    return unhealthyScore;
  }

  /**
   * Adjusts the unhealthy member of the ServerConnection.
   *
   * @param unhealthyScore how unhealthy the connection to the server is as
   * judged by the HeartbeatMonitor
   * @param connection server to adjust connection health for
   */
  private void adjustConnectionHealth(int unhealthyScore,
      ServerConnection connection) {
    if (unhealthyScore >= 2) {
      connection.incrementUnhealthy();
    } else {
      connection.decrementUnhealthy();
    }
  }

  /**
   * If the major heartbeat doesn't contain a file that the Controller believes
   * it should AND that specific file has already been added to the
   * missingChunks set, the Controller dispatches a message intended to replace
   * that file at the server.
   *
   * @param connection ServerConnection whose files to cross-reference
   * @param files list of files from most recent (major) heartbeat
   * @param missingChunks set of file:sequence pairs the Controller has noticed
   * might be missing from the ChunkServer
   */
  private void replaceMissingFiles(ServerConnection connection,
      ArrayList<FileMetadata> files,
      HashSet<Map.Entry<String,Integer>> missingChunks) {
    Set<Map.Entry<String,Integer>> chunksAtServer = createSetOfChunks(files);
    Map<String,ArrayList<Integer>> storedChunks = connection.getStoredChunks();
    for (String filename : storedChunks.keySet()) {
      ArrayList<Integer> sequences = storedChunks.get(filename);
      for (int sequence : sequences) {
        Map.Entry<String,Integer> entry = Map.entry(filename, sequence);
        if (!chunksAtServer.contains(entry)) {
          if (!missingChunks.contains(entry)) {
            logger.debug(
                filename + "_chunk" + sequence + " added to missingChunks.");
            missingChunks.add(entry);
          } else {
            logger.debug(
                "Dispatching repair for " + filename + "_chunk" + sequence);
            dispatchRepair(filename, sequence, connection.getServerAddress());
            missingChunks.remove(entry);
          }
        } else { // file stored at server, remove if exists from missingChunks
          missingChunks.remove(entry);
        }
      }
    }
  }

  /**
   * Creates a Set of filename:sequence tuples that are stored at the
   * ChunkServer.
   *
   * @param files list of FileMetadatas stored at the ChunkServer
   * @return HashSet made from the list of FileMetadatas
   */
  private Set<Map.Entry<String,Integer>> createSetOfChunks(
      ArrayList<FileMetadata> files) {
    ConcurrentHashMap<Map.Entry<String,Integer>,Integer> quickAddMap =
        new ConcurrentHashMap<>();
    files.parallelStream()
         .map(FileMetadata::getFilename)
         .forEach((filename) -> quickAddMap.put(
             Map.entry(FilenameUtilities.getBaseFilename(filename),
                 FilenameUtilities.getSequence(filename)), 0));
    return quickAddMap.keySet();
  }

  /**
   * Creates a repair message to replace the chunk with the filename/sequence
   * combo given as parameters. A different message has to be created whether
   * erasure coding or replicating.
   *
   * @param baseFilename base filename of chunk to be repaired
   * @param sequence sequence of chunk to be repaired
   * @param destination host:port address of server that needs replacement file
   */
  private void dispatchRepair(String baseFilename, int sequence,
      String destination) {
    String filename = baseFilename + "_chunk" + sequence;
    String[] servers = information.getServers(filename, sequence);

    RepairChunk message =
        ControllerInformation.makeRepairMessage(filename, servers, destination,
            new int[]{0, 1, 2, 3, 4, 5, 6, 7});
    if (message != null) {
      connectionCache.send(message.getAddress(), message, true, false);
    }
  }

  /**
   * Sends a poke message to a ChunkServer.
   *
   * @param connection to send poke message to
   * @return true if successfully sent, false if pipe is broken
   */
  private boolean pokeServer(ServerConnection connection) {
    String address = connection.getServerAddress();
    GeneralMessage message =
        new GeneralMessage(Protocol.CONTROLLER_SENDS_HEARTBEAT);
    boolean reachable = connectionCache.send(address, message, true, true);
    if (reachable) {
      connection.incrementPokes();
      return true;
    }
    logger.debug(address + " is unreachable, set to deregister.");
    return false;
  }

  /**
   * Method performed every time the HeartbeatMonitor's timer task is called.
   */
  public void run() {
    ArrayList<Integer> toDeregister = new ArrayList<>();
    synchronized(controller) { // to prevent modifications
      if (information.getRegisteredServers().isEmpty()) {
        return;
      }

      StringBuilder sb = (new StringBuilder()).append("\nHeartbeat Monitor:");
      long currentTime = System.currentTimeMillis();
      for (ServerConnection connection : information.getRegisteredServers()
                                                    .values()) {
        if (!pokeServer(connection)) {
          toDeregister.add(connection.getIdentifier());
          continue;
        }

        sb.append("\n").append(connection);
        HeartbeatInformation heartbeatInformation =
            connection.getHeartbeatInfo().copy();
        sb.append("\n")
          .append(createStringOfFiles(heartbeatInformation.getFiles()));

        adjustConnectionHealth(
            calculateUnhealthyScore(currentTime, connection.getStartTime(),
                heartbeatInformation), connection);
        if (connection.getUnhealthy() > 3) {
          toDeregister.add(connection.getIdentifier());
          continue;
        }

        // For a major heartbeat, replace missing files
        if (getLastHeartbeatType(currentTime, heartbeatInformation) == 1) {
          replaceMissingFiles(connection, heartbeatInformation.getFiles(),
              heartbeatInformation.getMissingChunks());
        }
      }

      // Print the contents of all heartbeats for registered ChunkServers
      logger.debug(sb.toString());
      controller.deregister(toDeregister);
    }
  }
}