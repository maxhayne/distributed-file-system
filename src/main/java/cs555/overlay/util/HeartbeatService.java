package cs555.overlay.util;

import cs555.overlay.node.ChunkServer;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to be encapsulated in a TimerTask to be called by the ChunkServer.
 *
 * @author hayne
 */
public class HeartbeatService extends TimerTask {

  private static final Logger logger = Logger.getInstance();
  private final ChunkServer chunkServer;
  private final ConcurrentHashMap<String, FileMetadata> majorFiles;
  private final ConcurrentHashMap<String, FileMetadata> minorFiles;
  private int heartbeatNumber;

  public HeartbeatService(ChunkServer chunkServer) {
    this.chunkServer = chunkServer;
    this.majorFiles = new ConcurrentHashMap<>();
    this.minorFiles = new ConcurrentHashMap<>();
    this.heartbeatNumber = 1;
  }

  /**
   * Performs a heartbeat for the ChunkServer, either major or minor. Gets a
   * list of files in the ChunkServer's directory using listFiles(). Every file
   * returned by listFiles that is also in the ChunkServer's 'files' FileMap is
   * added to the list of FileMetadatas.
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
    ConcurrentHashMap<String, FileMetadata> metadataMap =
        chunkServer.getFiles().getMap();
    chunkServer.getFileSynchronizer()
               .listFiles()
               .parallelStream()
               .forEach( (filename) -> {
                 FileMetadata metadata = metadataMap.get( filename );
                 if ( (beatType == 0 && !majorFiles.containsKey( filename )) ||
                      beatType == 1 ) {
                   if ( metadata != null ) {
                     fileMap.put( filename, metadata );
                   }
                 }
               } );
    /*
    for ( String filename : chunk ) {
      // Skip files that aren't in the fileMap
      // If a minor heartbeat, skip files already added to majorFiles
      FileMetadata metadata = files.getMap().get( filename );
      if ( metadata == null ||
           (beatType == 0 && majorFiles.containsKey( filename )) ) {
        continue;
      }
      fileMap.put( filename, metadata );
    }
     */

    // If minor heartbeat, put all files that were new, and added to
    // minorFiles (fileMap) into majorFiles
    if ( beatType == 0 ) {
      majorFiles.putAll( fileMap );
    }

    // Create heartbeat message, and return bytes of that message
    int totalChunks = majorFiles.size();
    long freeSpace = chunkServer.getFileSynchronizer().getUsableSpace();
    // Although the FileMetadatas inside the majorFiles and minorFiles are
    // from the 'files' ConcurrentHashMap in the ChunkServer, our copy of the
    // FileMetadata won't be deleted, only modified. Entries mapping to the
    // FileMetadata may be removed while we're sending a heartbeat message
    // out, but since we have the reference, the maps in here will remain the
    // same size.
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

  /**
   * The method that will be run every time the ChunkServer is scheduled to
   * perform a heartbeat.
   */
  public void run() {
    try {
      byte[] heartbeat =
          heartbeatNumber%10 == 0 ? heartbeat( 1 ) : heartbeat( 0 );
      assert heartbeat != null;
      chunkServer.getControllerConnection().getSender().sendData( heartbeat );
      logger.debug( "Heartbeat "+heartbeatNumber+" sent to the Controller at "+
                    new Date() );
    } catch ( NoSuchAlgorithmException nsae ) {
      logger.error( nsae.getMessage() );
    } catch ( IOException ioe ) {
      logger.debug(
          "Unable to send heartbeat to Controller. "+ioe.getMessage() );
      // Could just deregister here...

    } catch ( AssertionError ae ) {
      logger.debug( "Heartbeat couldn't be constructed. "+ae.getMessage() );
    } finally {
      heartbeatNumber++;
    }
  }
}