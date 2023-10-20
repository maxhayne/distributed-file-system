package cs555.overlay.util;

import cs555.overlay.files.FileReader;
import cs555.overlay.files.FileReaderFactory;
import cs555.overlay.node.ChunkServer;
import cs555.overlay.wireformats.ChunkServerReportsFileCorruption;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HeartbeatService extends TimerTask {

  private final ChunkServer chunkServer;
  private final Map<String, FileMetadata> majorFiles;
  private final Map<String, FileMetadata> minorFiles;
  private int heartbeatNumber;

  public HeartbeatService(ChunkServer chunkServer) {
    this.chunkServer = chunkServer;
    this.majorFiles = new HashMap<String, FileMetadata>();
    this.minorFiles = new HashMap<String, FileMetadata>();
    this.heartbeatNumber = 1;
  }

  /**
   * Takes byte string that has been read from file with name 'filename', and a
   * length, and resizes the file and byte string if the byte string is longer
   * than the length parameter.
   *
   * @param filename name of file fileBytes has been read from
   * @param fileBytes byte content of file filename
   * @param length to truncate file and fileBytes to
   * @return truncated fileBytes
   */
  private byte[] truncateIfNecessary(String filename, byte[] fileBytes,
      int length) {
    if ( fileBytes.length > length ) {
      chunkServer.getFileService().truncateFile( filename, length );
      byte[] truncatedBytes = new byte[length];
      System.arraycopy( fileBytes, 0, truncatedBytes, 0, length );
      fileBytes = truncatedBytes;
    }
    return fileBytes;
  }

  /**
   * Performs a heartbeat for the ChunkServer, either major or minor. Loops
   * through the list of files in the ChunkServer's directory, checks that they
   * aren't corrupt, extracts metadata from them, and constructs a heartbeat
   * message from that metadata. Returns a byte string message that is ready to
   * be sent to the Controller as a heartbeat.
   *
   * @param beatType 1 for major, 0 for minor
   * @return byte string of heartbeat message, or null if couldn't be created
   * @throws IOException if a file could not be read
   * @throws NoSuchAlgorithmException if SHA1 isn't available
   */
  private byte[] heartbeat(int beatType)
      throws IOException, NoSuchAlgorithmException {
    // fileMap will be the map that we are modifying during this heartbeat.
    // If minorHeartbeat, fileMap = minorFiles, if majorHeartbeat, fileMap =
    // majorFiles
    Map<String, FileMetadata> fileMap = beatType == 0 ? minorFiles : majorFiles;
    fileMap.clear();
    String[] files = chunkServer.getFileService().listFiles();
    // loop through list of filenames stored at ChunkServer
    for ( String filename : files ) {
      // if this is a minor heartbeat, skip files that have already been
      // added to majorFiles
      if ( beatType == 0 && majorFiles.containsKey( filename ) ) {
        continue;
      }

      FileReaderFactory factory = FileReaderFactory.getInstance();
      FileReader fileReader = factory.createFileReader( filename );
      fileReader.readAndProcess( chunkServer.getFileService() );

      if ( fileReader.isCorrupt() ) {
        ChunkServerReportsFileCorruption report =
            new ChunkServerReportsFileCorruption( chunkServer.getIdentifier(),
                filename, fileReader.getCorruption() );
        try {
          chunkServer.getControllerConnection()
                     .getSender()
                     .sendData( report.getBytes() );
        } catch ( IOException ioe ) {
          System.out.println(
              "heartbeat: Couldn't notify the Controller of corruption. "+
              ioe.getMessage() );
        }
      } else {
        fileMap.put( filename, fileReader.getMetadata() );
      }
    }

    // if minor heartbeat, put all files that were new, and added to
    // minorFiles (fileMap) into majorFiles
    if ( beatType == 0 ) {
      majorFiles.putAll( fileMap );
    }

    // create heartbeat message, and return bytes of that message
    int totalChunks = majorFiles.size();
    long freeSpace = chunkServer.getFileService().getUsableSpace();
    ChunkServerSendsHeartbeat heartbeat =
        new ChunkServerSendsHeartbeat( chunkServer.getIdentifier(), beatType,
            totalChunks, freeSpace, new ArrayList<>( fileMap.values() ) );
    try {
      return heartbeat.getBytes();
    } catch ( IOException ioe ) {
      System.out.println(
          "heartbeat: Unable to create heartbeat message. "+ioe.getMessage() );
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
      byte[] heartbeatMessage =
          heartbeatNumber%10 == 0 ? heartbeat( 1 ) : heartbeat( 0 );
      assert heartbeatMessage != null;
      chunkServer.getControllerConnection()
                 .getSender()
                 .sendData( heartbeatMessage );
      System.out.println(
          "Heartbeat "+heartbeatNumber+" has been sent to the Controller at "+
          new Date() );

    } catch ( NoSuchAlgorithmException nsae ) {
      System.err.println( "HeartbeatService::run: NoSuchAlgorithmException. "+
                          nsae.getMessage() );
    } catch ( IOException ioe ) {
      System.err.println(
          "HeartbeatService::run: Unable to send heartbeat to Controller. "+
          ioe.getMessage() );
    } catch ( AssertionError ae ) {
      System.err.println( "HeartbeatService::run: heartbeatMessage could not "+
                          "be constructed. "+ae.getMessage() );
    } finally {
      heartbeatNumber++;
    }
  }
}