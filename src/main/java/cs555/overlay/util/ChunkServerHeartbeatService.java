package cs555.overlay.util;

import cs555.overlay.node.ChunkServer;
import cs555.overlay.wireformats.ChunkServerReportsFileCorruption;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ChunkServerHeartbeatService extends TimerTask {

    private final ChunkServer chunkServer;
    private final Map<String, FileMetadata> majorFiles;
    private final Map<String, FileMetadata> minorFiles;
    private int heartbeatNumber;

    public ChunkServerHeartbeatService( ChunkServer chunkServer ) {
        this.chunkServer = chunkServer;
        this.majorFiles = new HashMap<String, FileMetadata>();
        this.minorFiles = new HashMap<String, FileMetadata>();
        this.heartbeatNumber = 1;
    }

    /**
     * Takes byte string that has been read from file with name 'filename', and
     * a length, and resizes the file and byte string if the byte string is
     * longer than the length parameter.
     *
     * @param filename name of file fileBytes has been read from
     * @param fileBytes byte content of file filename
     * @param length to truncate file and fileBytes to
     * @return truncated fileBytes
     */
    private byte[] truncateIfNecessary( String filename, byte[] fileBytes,
        int length ) {
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
     * through the list of files in the ChunkServer's directory, checks that
     * they aren't corrupt, extracts metadata from them, and constructs a
     * heartbeat message from that metadata. Returns a byte string message that
     * is ready to be sent to the Controller as a heartbeat.
     *
     * @param beatType 1 for major, 0 for minor
     * @return byte string of heartbeat message, or null if couldn't be created
     * @throws IOException if a file could not be read
     * @throws NoSuchAlgorithmException if SHA1 isn't available
     */
    private byte[] heartbeat( int beatType )
        throws IOException, NoSuchAlgorithmException {
        // fileMap will be the map that we are modifying during this
        // heartbeat. If minorHeartbeat, fileMap = minorFiles, if
        // majorHeartbeat, fileMap = majorFiles
        Map<String, FileMetadata> fileMap =
            beatType == 0 ? minorFiles : majorFiles;
        fileMap.clear();
        String[] files = chunkServer.getFileService().listFiles();
        // loop through list of filenames stored at ChunkServer
        for ( String filename : files ) {
            // if this is a minor heartbeat, skip files that have already
            // been added to majorFiles
            if ( beatType == 0 && majorFiles.containsKey( filename ) ) {
                continue;
            }
            // read the file
            byte[] fileBytes =
                chunkServer.getFileService().readBytesFromFile( filename );
            if ( !FileDistributionService.checkShardFilename( filename ) ) {
                // it is a chunk
                // if file is too large for a chunk, truncate it to proper
                // length, and truncate byte string already read
                fileBytes = truncateIfNecessary( filename, fileBytes, 65720 );

                // check if chunk is corrupt
                Vector<Integer> corruptSlices =
                    FileDistributionService.checkChunkForCorruption(
                        fileBytes );
                int[] corruptArray = null;
                if ( !corruptSlices.isEmpty() ) {
                    corruptArray = ChunkServer.vecToArr( corruptSlices );
                }

                // if chunk isn't corrupt, add to fileMap
                if ( corruptArray == null ) {
                    ByteBuffer buffer = ByteBuffer.wrap( fileBytes );
                    fileMap.put( filename,
                        new FileMetadata( filename, buffer.getInt( 28 ),
                            buffer.getLong( 36 ) ) );
                    // version starts at 28, timestamp starts at 36
                } else { // chunk is corrupt
                    // add corruption event to fileService queue
                    ChunkServerReportsFileCorruption event =
                        new ChunkServerReportsFileCorruption(
                            chunkServer.getIdentifier(), filename,
                            corruptArray );
                    chunkServer.getFileService().addToQueue( event );
                }
            } else { // it is a shard
                // if shard is too long, truncate both the file and the bytes
                // that have already been read
                fileBytes = truncateIfNecessary( filename, fileBytes, 10994 );

                // check if shard is corrupt
                boolean corrupt =
                    FileDistributionService.checkShardForCorruption(
                        fileBytes );

                // if corrupt, add corruption event to fileService queue
                if ( corrupt ) {
                    ChunkServerReportsFileCorruption event =
                        new ChunkServerReportsFileCorruption(
                            chunkServer.getIdentifier(), filename, null );
                    chunkServer.getFileService().addToQueue( event );
                } else { // if not corrupt, add to fileMap
                    ByteBuffer buffer = ByteBuffer.wrap( fileBytes );
                    fileMap.put( filename,
                        new FileMetadata( filename, buffer.getInt( 28 ),
                            buffer.getLong( 32 ) ) );
                }
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
            new ChunkServerSendsHeartbeat( chunkServer.getIdentifier(), 0,
                totalChunks, freeSpace, new ArrayList<>( fileMap.values() ) );
        try {
            return heartbeat.getBytes();
        } catch ( IOException ioe ) {
            System.out.println(
                "heartbeat: Unable to create heartbeat message. "
                + ioe.getMessage() );
            // return null if cannot get message bytes
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
                heartbeatNumber % 10 == 0 ? heartbeat( 1 ) : heartbeat( 0 );

            if ( heartbeatMessage != null ) {
                chunkServer.getControllerConnection()
                    .getSender()
                    .sendData( heartbeatMessage );
                System.out.println( "Heartbeat " + heartbeatNumber
                                    + " has been sent to the Controller at "
                                    + new Date() );
            } else {
                System.out.println( "ChunkServerHeartbeatService::run: "
                                    + " null heartbeatMessage was not sent "
                                    + "to the Controller." );
            }
            heartbeatNumber += 1;
        } catch ( NoSuchAlgorithmException nsae ) {
            System.err.println( "ChunkServerHeartbeatService::run: "
                                + "NoSuchAlgorithmException. "
                                + nsae.getMessage() );
        } catch ( IOException ioe ) {
            System.err.println( "ChunkServerHeartbeatService::run: Unable to "
                                + "send heartbeat to Controller. "
                                + ioe.getMessage() );
        }
    }
}