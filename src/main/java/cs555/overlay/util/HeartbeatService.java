package cs555.overlay.util;

import cs555.overlay.node.ChunkServer;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Class to be encapsulated in a TimerTask to be called by the ChunkServer.
 *
 * @author hayne
 */
public class HeartbeatService extends TimerTask {

  private static final Logger logger = Logger.getInstance();
  private final ChunkServer chunkServer;
  private final Map<String, FileMetadata> majorFiles;
  private final Map<String, FileMetadata> minorFiles;
  private int heartbeatNumber;

  public HeartbeatService(ChunkServer chunkServer) {
    this.chunkServer = chunkServer;
    this.majorFiles = new HashMap<>();
    this.minorFiles = new HashMap<>();
    this.heartbeatNumber = 1;
  }

  /**
   * Performs a heartbeat for the ChunkServer, either major or minor. Gets a
   * list of files in the ChunkServer's directory using listFiles(). Every file
   * returned by listFiles that is also in the ChunkServer's 'files' FileMap is
   * added to the list of FileMetadatas
   *
   * @param beatType 1 for major, 0 for minor
   * @return byte string of heartbeat message, or null if couldn't be created
   * @throws IOException if a file could not be read
   * @throws NoSuchAlgorithmException if SHA1 isn't available
   */
  private byte[] heartbeat(int beatType)
      throws IOException, NoSuchAlgorithmException {
    // fileMap is the map that we are modifying in this heartbeat.
    // minorHeartbeat fileMap = minorFiles, majorHeartbeat fileMap = majorFiles
    Map<String, FileMetadata> fileMap = beatType == 0 ? minorFiles : majorFiles;
    fileMap.clear();
    String[] filesInFolder = chunkServer.getFileSynchronizer().listFiles();
    FileMap files = chunkServer.getFiles();
    synchronized( files ) {
      for ( String filename : filesInFolder ) {
        // skip files that aren't in the fileMap
        // if a minor heartbeat, skip files already added to majorFiles
        if ( !files.getFiles().containsKey( filename ) ||
             (beatType == 0 && majorFiles.containsKey( filename )) ) {
          continue;
        }
        fileMap.put( filename, files.getFiles().get( filename ) );
      }

      // if minor heartbeat, put all files that were new, and added to
      // minorFiles (fileMap) into majorFiles
      if ( beatType == 0 ) {
        majorFiles.putAll( fileMap );
      }

      // create heartbeat message, and return bytes of that message
      int totalChunks = majorFiles.size();
      long freeSpace = chunkServer.getFileSynchronizer().getUsableSpace();
      ChunkServerSendsHeartbeat heartbeat =
          new ChunkServerSendsHeartbeat( chunkServer.getIdentifier(), beatType,
              totalChunks, freeSpace, new ArrayList<>( fileMap.values() ) );
      try {
        return heartbeat.getBytes();
      } catch ( IOException ioe ) {
        logger.debug( "Unable to create heartbeat message. "+ioe.getMessage() );
        // return null if message bytes can't be constructed
        return null;
      }
    }
  }

  /**
   * The method that will be run every time the ChunkServer is scheduled to
   * perform a heartbeat.
   */
  public void run() {
    try {
      byte[] heartbeatMessage =
          heartbeatNumber%10 == 0 ? heartbeat( 1 ) : heartbeat( 0 );
      assert heartbeatMessage != null;
      chunkServer.getControllerConnection()
                 .getSender()
                 .sendData( heartbeatMessage );
      logger.debug( "Heartbeat "+heartbeatNumber+" sent to the Controller at "+
                    new Date() );
    } catch ( NoSuchAlgorithmException nsae ) {
      logger.error( nsae.getMessage() );
    } catch ( IOException ioe ) {
      logger.debug(
          "Unable to send heartbeat to Controller. "+ioe.getMessage() );
    } catch ( AssertionError ae ) {
      logger.debug( "Heartbeat couldn't be constructed. "+ae.getMessage() );
    } finally {
      heartbeatNumber++;
    }
  }
}