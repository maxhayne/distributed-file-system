package cs555.overlay.util;

import cs555.overlay.node.ChunkServer;
import cs555.overlay.node.Client;
import cs555.overlay.transport.TCPSender;
import cs555.overlay.wireformats.*;
import erasure.ReedSolomon;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class FileDistributionService implements Runnable {

    private final ChunkServer chunkServer;
    private final Path directory;
    private final BlockingQueue<Event> eventQueue;
    private boolean activeStatus;

    public FileDistributionService( ChunkServer chunkServer )
        throws IOException {

        this.chunkServer = chunkServer;
        this.directory = Paths.get( File.separator, "tmp",
            "ChunkServer" + "-" + chunkServer.getIdentifier() );

        Files.createDirectories( directory );

        this.eventQueue = new LinkedBlockingQueue<Event>();
        this.activeStatus = false;
    }

    public Path getDirectory() {
        return directory;
    }

    public static double getFileSize( String filename ) {
        return ( new File( filename ) ).length();
    }

    public long getUsableSpace() {
        return ( new File( directory.toString() ) ).getUsableSpace();
    }

    public String[] listFiles() throws IOException {
        try ( Stream<Path> stream = Files.list( directory ) ) {
            return ( String[] ) stream.filter(
                    file -> !Files.isDirectory( file ) )
                                    .map( Path::getFileName )
                                    .map( Path::toString )
                                    .filter( file -> checkChunkFilename( file )
                                                     || checkShardFilename(
                                        file ) )
                                    .toArray();
        }
    }

    public String getServerAddress() {
        return chunkServer.getHost() + ":" + chunkServer.getPort();
    }

    public static boolean checkChunkFilename( String filename ) {
        boolean matches = filename.matches( ".*_chunk[0-9]*$" );
        String[] split = filename.split( "_chunk" );
        if ( matches && split.length == 2 ) {
            return true;
        }
        return false;
    }

    public static boolean checkShardFilename( String filename ) {
        boolean matches = filename.matches( ".*_chunk[0-9]*_shard[0-8]$" );
        String[] split1 = filename.split( "_chunk" );
        String[] split2 = filename.split( "_shard" );
        if ( matches && split1.length == 2 && split2.length == 2 ) {
            return true;
        }
        return false;
    }

    // Function for generating hash
    public static byte[] SHA1FromBytes( byte[] data )
        throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance( "SHA1" );
        return digest.digest( data );
    }

    public static byte[][] makeShardsFromChunk( byte[] chunk ) {
        if ( chunk.length != 65720 ) {
            return null;
        }
        int fileSize = 65720;
        int storedSize = fileSize + Constants.BYTES_IN_INT;
        int shardSize = storedSize / Constants.DATA_SHARDS;
        int bufferSize = shardSize * Constants.DATA_SHARDS;
        byte[] allBytes = new byte[bufferSize];
        ByteBuffer allBytesBuffer = ByteBuffer.wrap( allBytes );
        allBytesBuffer.putInt( chunk.length );
        allBytesBuffer.put( chunk );
        byte[][] shards = new byte[Constants.TOTAL_SHARDS][shardSize];
        for ( int i = 0; i < Constants.DATA_SHARDS; i++ ) {
            System.arraycopy( allBytes, i * shardSize, shards[i], 0,
                shardSize );
        }
        ReedSolomon reedSolomon =
            new ReedSolomon( Constants.DATA_SHARDS, Constants.PARITY_SHARDS );
        reedSolomon.encodeParity( shards, 0, shardSize );
        return shards;
    }

    public static byte[][] decodeMissingShards( byte[][] shards ) {
        if ( shards.length != Constants.TOTAL_SHARDS ) {
            return null;
        }
        boolean[] shardPresent = new boolean[Constants.TOTAL_SHARDS];
        int shardCount = 0;
        int shardSize = 0;
        for ( int i = 0; i < Constants.TOTAL_SHARDS; i++ ) {
            if ( shards[i] != null ) {
                shardPresent[i] = true;
                shardCount++;
                shardSize = shards[i].length;
            }
        }
        if ( shardCount < Constants.DATA_SHARDS ) {
            return null;
        }
        for ( int i = 0; i < Constants.TOTAL_SHARDS; i++ ) {
            if ( !shardPresent[i] ) {
                shards[i] = new byte[shardSize];
            }
        }
        ReedSolomon reedSolomon =
            new ReedSolomon( Constants.DATA_SHARDS, Constants.PARITY_SHARDS );
        reedSolomon.decodeMissing( shards, shardPresent, 0, shardSize );
        return shards;
    }

    public static byte[] getChunkFromShards( byte[][] shards ) {
        int shardSize = shards[0].length;
        byte[] decodedChunk = new byte[shardSize * Constants.DATA_SHARDS];
        for ( int i = 0; i < Constants.DATA_SHARDS; i++ ) {
            System.arraycopy( shards[i], 0, decodedChunk, shardSize * i,
                shardSize );
        }
        return Arrays.copyOfRange( decodedChunk, 4,
            65724 ); // Will then have to removeHashesFromChunk
        // (correctedDecode) and getDataFromChunk()
    }

    public static synchronized byte[] getNextChunkFromFile( String filename,
        int sequence ) {
        int position = sequence * 65536;
        try ( RandomAccessFile file = new RandomAccessFile( filename, "r" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock( position, 65536, true ) ) {
            if ( position > channel.size() ) {
                return null;
            }
            byte[] data = ( channel.size() - position ) < 65536 ?
                              new byte[( int ) channel.size() - position] :
                              new byte[65536];
            ByteBuffer buffer = ByteBuffer.wrap( data );
            int read = 1;
            while ( buffer.hasRemaining() && read > 0 ) {
                read = channel.read( buffer, position );
                position += read;
            }
            return data;
        } catch ( IOException ioe ) {
            return null;
        }
    }

    public synchronized void truncateFile( String filename, long size ) {
        try ( RandomAccessFile file = new RandomAccessFile( filename, "rw" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock() ) {
            channel.truncate( size );
        } catch ( IOException ioe ) {
            System.out.println(
                "truncateFile: Error truncating '" + filename + "'. "
                + ioe.getMessage() );
        }
    }

    /**
     * Takes a filename and data, and creates a byte[] which is ready to be
     * written to disk as a file.
     *
     * @param filename of file to be stored
     * @param data byte[] of chunk or shard
     * @return byte[] of file data, null if unsuccessful
     */
    public static byte[] readyFileForStorage( String filename, byte[] data ) {
        byte[] fileBytes;
        if ( checkChunkFilename( filename ) ) {
            int sequence = Integer.parseInt( filename.split( "_chunk" )[1] );
            fileBytes = readyChunkForStorage( sequence, 0, data );
        } else if ( checkShardFilename( filename ) ) {
            String[] split = filename.split( "_shard" );
            int fragment = Integer.parseInt( split[1] );
            int sequence = Integer.parseInt( split[0].split( "_chunk" )[1] );
            fileBytes = readyShardForStorage( sequence, fragment, 0, data );
        } else {
            System.out.println( "readyFileForStorage: '" + filename
                                + "' is neither a chunk nor a shard. It "
                                + "cannot be converted"
                                + " into a byte[] for storage." );
            return null;
        }
        return fileBytes;
    }

    // Takes chunk data, combines with metadata and hashes, basically
    // prepares it for
    // writing to a file.
    public static byte[] readyChunkForStorage( int sequence, int version,
        byte[] chunkArray ) {
        int chunkArrayRemaining = chunkArray.length;
        byte[] chunkToFileArray = new byte[65720]; // total size of stored chunk
        byte[] sliceArray = new byte[8195];
        ByteBuffer chunkToFileBuffer = ByteBuffer.wrap( chunkToFileArray );
        ByteBuffer sliceBuffer = ByteBuffer.wrap( sliceArray );
        sliceBuffer.putInt( 0 ); // padding
        sliceBuffer.putInt( sequence );
        sliceBuffer.putInt( version );
        sliceBuffer.putInt( chunkArrayRemaining );
        sliceBuffer.putLong( System.currentTimeMillis() );
        int position = 0;
        if ( chunkArrayRemaining >= 8195 - 24 ) {
            sliceBuffer.put( chunkArray, position, 8195 - 24 );
            chunkArrayRemaining -= ( 8195 - 24 );
            position += ( 8195 - 24 );
        } else {
            sliceBuffer.put( chunkArray, 0, chunkArrayRemaining );
            chunkArrayRemaining = 0;
        }
        try {
            byte[] hash = SHA1FromBytes( sliceArray );
            chunkToFileBuffer.put( hash );
            chunkToFileBuffer.put( sliceArray );
            sliceBuffer.clear();
            Arrays.fill( sliceArray, ( byte ) 0 );
            for ( int i = 0; i < 7; i++ ) {
                if ( chunkArrayRemaining == 0 ) {
                    hash = SHA1FromBytes( sliceArray );
                } else if ( chunkArrayRemaining < 8195 ) {
                    sliceBuffer.put( chunkArray, position,
                        chunkArrayRemaining );
                    chunkArrayRemaining = 0;
                    hash = SHA1FromBytes( sliceArray );
                } else {
                    sliceBuffer.put( chunkArray, position, 8195 );
                    chunkArrayRemaining -= 8195;
                    position += 8195;
                    hash = SHA1FromBytes( sliceArray );
                }
                chunkToFileBuffer.put( hash );
                chunkToFileBuffer.put( sliceArray );
                sliceBuffer.clear();
                Arrays.fill( sliceArray, ( byte ) 0 );
            }
        } catch ( NoSuchAlgorithmException nsae ) {
            System.err.println(
                "readyChunkForStorage: Can't access algorithm for SHA1." );
            return null;
        }
        return chunkToFileArray;
    }

    public static byte[] readyShardForStorage( int sequence, int fragment,
        int version, byte[] shardArray ) {
        byte[] shardToFileArray = new byte[20 + ( 3 * Constants.BYTES_IN_INT )
                                           + Constants.BYTES_IN_LONG
                                           + 10954]; // Hash+Sequence
        // +Fragment+Version+Timestamp+Shard, 10994 bytes in total
        byte[] shardWithMetaData =
            new byte[( 3 * Constants.BYTES_IN_INT ) + Constants.BYTES_IN_LONG
                     + 10954];
        ByteBuffer shardMetaWrap = ByteBuffer.wrap( shardWithMetaData );
        shardMetaWrap.putInt( sequence );
        shardMetaWrap.putInt( fragment );
        shardMetaWrap.putInt( version );
        shardMetaWrap.putLong( System.currentTimeMillis() );
        shardMetaWrap.put( shardArray );
        byte[] hash = null;
        try {
            hash = SHA1FromBytes( shardWithMetaData );
            ByteBuffer shardFileArrayWrap = ByteBuffer.wrap( shardToFileArray );
            shardFileArrayWrap.put( hash );
            shardFileArrayWrap.put( shardWithMetaData );
            return shardToFileArray;
        } catch ( NoSuchAlgorithmException nsae ) {
            System.err.println(
                "readyShardForStorage: Can't access algorithm for SHA1." );
            return null;
        }
    }

    // Read any file and return a byte[] of the data
    public synchronized byte[] readBytesFromFile( String filename ) {
        byte[] fileBytes;
        try {
            fileBytes =
                Files.readAllBytes( getDirectory().resolve( filename ) );
        } catch ( IOException ioe ) {
            System.out.println(
                "readBytesFromFile: Unable to read '" + filename + "'."
                + ioe.getMessage() );
            return null;
        }
        return fileBytes;
        /*
        File tryFile = new File( filename );
        if ( !tryFile.isFile() ) {
            return null;
        }
        try ( RandomAccessFile file = new RandomAccessFile( filename, "r" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock( 0, Long.MAX_VALUE, true ) ) {
            byte[] data = new byte[( int ) channel.size()];
            ByteBuffer buffer = ByteBuffer.wrap( data );
            int read = 1;
            int position = 0;
            while ( buffer.hasRemaining() && read > 0 ) {
                read = channel.read( buffer, position );
                position += read;
            }
            return data;
        } catch ( IOException ioe ) {
            return null;
        }
        */
    }

    // Check chunk for errors and return integer array containing slice numbers
    public static Vector<Integer> checkChunkForCorruption( byte[] chunkArray ) {
        Vector<Integer> corrupt = new Vector<Integer>();
        for ( int i = 0; i < 8; ++i ) {
            corrupt.add( i );
        }
        if ( chunkArray == null ) {
            return corrupt;
        }
        ByteBuffer chunk = ByteBuffer.wrap( chunkArray );
        byte[] hash = new byte[20];
        byte[] slice = new byte[8195];
        try {
            for ( int i = 0; i < 8; i++ ) {
                chunk.get( hash );
                chunk.get( slice );
                byte[] computedHash = SHA1FromBytes( slice );
                if ( Arrays.equals( hash, computedHash ) ) {
                    corrupt.removeElement( i );
                }
                Arrays.fill( hash, ( byte ) 0 );
                Arrays.fill( slice, ( byte ) 0 );
            }
        } catch ( BufferUnderflowException bue ) {
            // The array wasn't the correct length for a chunk
            System.err.println( "checkChunkCorruption: Byte string wasn't "
                                + "formatted properly." );
            //return null;
        } catch ( NoSuchAlgorithmException nsae ) {
            System.err.println( "checkChunkCorruption: Couldn't use SHA1." );
            //return null;
        }
        // The array could pass all the tests and still be corrupt, if any
        // information was added to the end of the file storing the chunk.
        // Correct length of the file is 65720 bytes.
        return corrupt;
    }

    public static boolean checkShardForCorruption( byte[] shardArray ) {
        if ( shardArray == null ) {
            return true;
        }
        ByteBuffer shardArrayBuffer = ByteBuffer.wrap( shardArray );
        boolean corrupt = false;
        byte[] hash = new byte[20];
        byte[] shard =
            new byte[( 3 * Constants.BYTES_IN_INT ) + Constants.BYTES_IN_LONG
                     + 10954];
        try {
            shardArrayBuffer.get( hash );
            shardArrayBuffer.get( shard );
            byte[] computedHash = SHA1FromBytes( shard );
            if ( Arrays.equals( hash, computedHash ) ) {
                return false;
            }
        } catch ( BufferUnderflowException bue ) {
            return true;
        } catch ( NoSuchAlgorithmException nsae ) {
            System.err.println( "checkShardForCorruption: Couldn't use SHA1." );
            return true;
        }
        return true;
    }

    // Removes hashes from chunk
    public static byte[] removeHashesFromChunk( byte[] chunkArray ) {
        ByteBuffer chunk = ByteBuffer.wrap( chunkArray );
        byte[] cleanedChunk = new byte[65560];
        for ( int i = 0; i < 8; i++ ) {
            chunk.position( chunk.position() + 20 );
            chunk.get( cleanedChunk, i * 8195, 8195 );
        }
        return cleanedChunk;
        // cleanedChunk will start like this:
        // Padding (0 int)
        // Sequence (int)
        // Version (int)
        // Total bytes of data in this chunk (int)
        // Timestamp (long)
    }

    // Removes hash from shard
    public static byte[] removeHashFromShard( byte[] shardArray ) {
        ByteBuffer shard = ByteBuffer.wrap( shardArray );
        byte[] cleanedShard =
            new byte[( 3 * Constants.BYTES_IN_INT ) + Constants.BYTES_IN_LONG
                     + 10954];
        shard.position( shard.position() + 20 );
        shard.get( cleanedShard, 0, cleanedShard.length );
        return cleanedShard;
        // cleanedShard will start like this:
        // Sequence (int)
        // ShardNumber (int)
        // Version (int)
        // Timestamp (long)
        // Data
    }

    // Removes metadata, and strips padding from end of array
    public static byte[] getDataFromChunk( byte[] chunkArray ) {
        ByteBuffer chunk = ByteBuffer.wrap( chunkArray );
        int chunkLength = chunk.getInt( 12 );
        chunk.position( 24 );
        byte[] data = new byte[chunkLength];
        chunk.get( data );
        return data;
    }

    // Removes metadata from shard
    public static byte[] getDataFromShard( byte[] shardArray ) {
        ByteBuffer shard = ByteBuffer.wrap( shardArray );
        shard.position( 20 );
        byte[] data = new byte[10954];
        shard.get( data );
        return data;
    }

    // Create file if it doesn't exist, if it does exist, return false
    public synchronized boolean writeNewFile( String filename, byte[] data ) {
        File tryFile = new File( filename );
        if ( tryFile.isFile() ) {
            return false;
        }
        tryFile = null;
        try ( RandomAccessFile file = new RandomAccessFile( filename, "rw" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock() ) {
            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( buffer.hasRemaining() ) {
                channel.write( buffer );
            }
            return true;
        } catch ( IOException ioe ) {
            return false;
        }
    }

    // Write new file, replace if it already exists.
    public synchronized boolean overwriteNewFile( String filename,
        byte[] data ) {
        try ( RandomAccessFile file = new RandomAccessFile( filename, "rw" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock() ) {
            channel.truncate( 0 );
            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( buffer.hasRemaining() ) {
                channel.write( buffer );
            }
            return true;
        } catch ( IOException ioe ) {
            return false;
        }
    }

    // Create file if it doesn't exist, append file with data
    public static synchronized boolean appendFile( String filename,
        byte[] data ) {
        try ( RandomAccessFile file = new RandomAccessFile( filename, "rw" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock() ) {
            channel.position( channel.size() );
            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( buffer.hasRemaining() ) {
                channel.write( buffer );
            }
            return true;
        } catch ( IOException ioe ) {
            return false;
        }
    }

    public synchronized void deleteFile( String filename ) throws IOException {
        // if filename is a specific chunk or shard, try to delete it
        if ( checkChunkFilename( filename ) || checkShardFilename(
            filename ) ) {
            Files.deleteIfExists( directory.resolve( filename ) );
            return;
        }

        // if filename is a base (what would come before "_chunk" or
        // "_shard"), then delete all chunks or shards with that base
        String[] files = listFiles();
        for ( String file : files ) {
            if ( file.split( "_chunk" )[0].equals( filename ) ) {
                Files.deleteIfExists( directory.resolve( file ) );
            }
        }
    }

    // Replace slices with new slices
    public synchronized boolean replaceSlices( String filename, int[] slices,
        byte[][] sliceData ) {
        File tryFile = new File( filename );
        if ( !tryFile.isFile() ) {
            return false; // can't replace slices for a file that doesn't exist
        }
        tryFile = null;
        try ( RandomAccessFile file = new RandomAccessFile( filename, "rw" );
              FileChannel channel = file.getChannel();
              FileLock lock = channel.lock() ) {
            int numSlices = slices.length;
            for ( int i = 0; i < numSlices; i++ ) {
                int position = 8215 * slices[i]; // includes the hashes
                ByteBuffer newSlice = ByteBuffer.wrap( sliceData[i] );
                while ( newSlice.hasRemaining() ) {
                    position += channel.write( newSlice, position );
                }
            }
            return true;
        } catch ( IOException ioe ) {
            return false;
        }
    }

    // Replace slices with new slices
    public byte[][] getSlices( byte[] chunkArray, int[] slices ) {
        int numSlices = slices.length;
        byte[][] sliceData = new byte[numSlices][8195];
        for ( int i = 0; i < numSlices; i++ ) {
            ByteBuffer buffer = ByteBuffer.wrap( sliceData[i] );
            buffer.put( chunkArray,
                ( 20 * ( slices[i] + 1 ) ) + slices[i] * 8195, 8195 );
        }
        return sliceData;
    }

    public byte[] getFileFromServer( String filename, TCPSender sender ) {
        try {
            GeneralMessage request =
                new GeneralMessage( Protocol.CHUNK_SERVER_REQUESTS_FILE,
                    filename );
            sender.sendData( request.getBytes() );
            byte[] filedata = sender.receiveData();
            if ( filedata == null
                 || filedata[0] == Protocol.CHUNK_SERVER_DENIES_REQUEST ) {
                //System.out.println("NULL");
                return null;
            }
            ChunkServerServesFile msg = new ChunkServerServesFile( filedata );
            //System.out.println(msg.filedata.length);
            return msg.filedata;
        } catch ( Exception e ) {
            return null;
        }
    }

    public void addToQueue( Event event ) {
        eventQueue.add( event );
    }

    public synchronized void setActiveStatus( boolean status ) {
        activeStatus = status;
    }

    public synchronized boolean getActiveStatus() {
        return activeStatus;
    }

    @Override
    public void run() {
        System.out.println( "FileDistributionService running." );
        Map<String, TCPSender> tcpConnections =
            new HashMap<String, TCPSender>();
        this.setActiveStatus( true );
        while ( this.getActiveStatus() ) {
            Event event = null;
            try {
                event = this.eventQueue.poll( 1, TimeUnit.SECONDS );
            } catch ( InterruptedException ie ) {
                System.err.println(
                    "FileDistributionService run InterruptedException: " + ie );
                continue;
            }
            if ( event == null ) {
                continue;
            }
            try {
                byte eventType = event.getType();
                if ( eventType
                     == Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION ) {
                    //System.out.println("File corruption event.");
                    ChunkServerReportsFileCorruption msg =
                        ( ChunkServerReportsFileCorruption ) event;
                    // Send message to controller about file corruption, wait
                    // for reply about where to
                    // find the replacements. Try the servers that have the
                    // replacements. If no server
                    // successfully can server the file, decide what to do next.
                    boolean fullyFixed = false;
                    byte[] reply = null;
                    TCPSender controllerSender =
                        Client.getTCPSender( tcpConnections,
                            ApplicationProperties.controllerHost + ":"
                            + String.valueOf(
                                ApplicationProperties.controllerPort ) );
                    if ( controllerSender == null ) {
                        continue;
                    }
                    // Get Storage list
                    try {
                        controllerSender.sendData( msg.getBytes() );
                        reply = controllerSender.receiveData();
                        if ( reply == null || reply[0]
                                              != Protocol.CONTROLLER_SENDS_STORAGE_LIST ) {
                            continue;
                        }
                    } catch ( Exception e ) {
                    }
                    ControllerSendsStorageList list =
                        new ControllerSendsStorageList( reply );
                    // Decide if we're a shard or a chunk
                    if ( checkShardFilename(
                        msg.filename ) ) { // We are a Shard
                        String[] servers = list.shardServers;
                        if ( servers == null ) {
                            continue;
                        }
                        // Get all shards you can, and try to reconstruct the
                        // shard you need from it.
                        byte[][] shards = new byte[Constants.TOTAL_SHARDS][];
                        int index = -1;
                        for ( String server : servers ) {
                            index++;
                            if ( server.equals( "-1" )
                                 || server.split( ":" ).length != 2 ) {
                                continue;
                            }
                            TCPSender sender =
                                Client.getTCPSender( tcpConnections, server );
                            if ( sender == null ) {
                                continue;
                            }
                            String shardname =
                                msg.filename.split( "_shard" )[0] + "_shard"
                                + String.valueOf( index );
                            byte[] filedata =
                                getFileFromServer( shardname, sender );
                            if ( filedata == null ) {
                                continue;
                            }
                            shards[index] = filedata;
                        }
                        byte[][] correctedShards =
                            decodeMissingShards( shards );
                        if ( correctedShards == null ) {
                            continue;
                        }
                        int sequence = Integer.valueOf(
                            msg.filename.split( "_shard" )[0].split(
                                "_chunk" )[1] );
                        int shardnumber = Integer.valueOf(
                            msg.filename.split( "_chunk" )[1].split(
                                "_shard" )[1] );
                        byte[] shardFileArray =
                            readyShardForStorage( sequence, shardnumber, 0,
                                correctedShards[shardnumber] );
                        overwriteNewFile( directory + msg.filename,
                            shardFileArray );
                        fullyFixed = true;
                    } else { // We are a Chunk
                        // Keep track of what we've repaired.
                        Vector<Integer> slicesToRepair = new Vector<Integer>();
                        for ( int i = 0; i < msg.slices.length; i++ ) {
                            slicesToRepair.add( msg.slices[i] );
                        }
                        if ( list.replicationServers == null ) {
                            continue;
                        }
                        for ( String replicationserver :
                            list.replicationServers ) {
                            if ( replicationserver.equals( "-1" )
                                 || replicationserver.split( ":" ).length
                                    != 2 ) {
                                continue;
                            }
                            if ( replicationserver.equals(
                                getServerAddress() ) ) {
                                continue; // Don't send request to self.
                            }
                            TCPSender slicesender =
                                Client.getTCPSender( tcpConnections,
                                    replicationserver );
                            if ( slicesender == null ) {
                                continue;
                            }
                            try {
                                int[] replaceslices =
                                    new int[slicesToRepair.size()];
                                for ( int i = 0;
                                      i < slicesToRepair.size(); i++ ) {
                                    replaceslices[i] =
                                        slicesToRepair.elementAt( i );
                                }
                                RequestsSlices request =
                                    new RequestsSlices( msg.filename,
                                        replaceslices );
                                slicesender.sendData( request.getBytes() );
                                byte[] slicereply = slicesender.receiveData();
                                if ( slicereply == null || slicereply[0]
                                                           != Protocol.CHUNK_SERVER_SERVES_SLICES ) {
                                    continue;
                                }
                                ChunkServerServesSlices serve =
                                    new ChunkServerServesSlices( slicereply );
                                int sliceLength = serve.slices.length;
                                byte[][] replacements =
                                    new byte[sliceLength][8215];
                                for ( int i = 0; i
                                                 < serve.slices.length; i++ ) { // add hashes to the data
                                    ByteBuffer buffer =
                                        ByteBuffer.wrap( replacements[i] );
                                    if ( serve.slices[i]
                                         == 0 ) { // increment the version
                                        ByteBuffer temp = ByteBuffer.wrap(
                                            serve.slicedata[i] );
                                        int version = temp.getInt( 8 );
                                        version++;
                                        temp.position( 8 );
                                        temp.putInt( version );
                                    }
                                    byte[] hash =
                                        SHA1FromBytes( serve.slicedata[i] );
                                    buffer.put( hash );
                                    buffer.put( serve.slicedata[i] );
                                }
                                // Replace the slices
                                replaceSlices( directory + msg.filename,
                                    serve.slices, replacements );
                                for ( int i = 0;
                                      i < serve.slices.length; i++ ) {
                                    slicesToRepair.removeElement(
                                        serve.slices[i] );
                                }
                                if ( slicesToRepair.size() == 0 ) {
                                    fullyFixed = true;
                                    break;
                                }
                            } catch ( Exception e ) {
                            } // Nothing to do here but to try to continue to
                            // the next server
                        }
                    }
                    if ( fullyFixed ) { // If it is fixed, tell the
                        // Controller that the chunk is healthy
                        try {
                            Socket controllerSocket = new Socket(
                                ApplicationProperties.controllerHost,
                                ApplicationProperties.controllerPort );
                            TCPSender sender =
                                new TCPSender( controllerSocket );
                            ChunkServerReportsFileFix fix =
                                new ChunkServerReportsFileFix(
                                    chunkServer.getIdentifier(), msg.filename );
                            sender.sendData( fix.getBytes() );
                            controllerSocket.close();
                            sender = null;
                        } catch ( Exception e ) {
                            // This is best effort.
                        }
                    }
                } else if ( eventType
                            == Protocol.CONTROLLER_REQUESTS_FILE_ACQUIRE ) {
                    //System.out.println("File aquisition event.");
                    ControllerRequestsFileAcquire msg =
                        ( ControllerRequestsFileAcquire ) event;
                    if ( checkChunkFilename( msg.filename ) ) { // It's a chunk
                        byte[] data =
                            readBytesFromFile( directory + msg.filename );
                        if ( data != null ) {
                            Vector<Integer> errors =
                                checkChunkForCorruption( data );
                            if ( errors.size() == 0 ) {
                                continue;
                            }
                        }
                        for ( String server : msg.servers ) {
                            if ( server.equals( "-1" )
                                 || server.split( ":" ).length != 2 ) {
                                //System.out.println("Server isn't an
                                // address: " + server);
                                continue;
                            }
                            TCPSender sender =
                                Client.getTCPSender( tcpConnections, server );
                            if ( sender == null ) {
                                continue;
                            }
                            byte[] filedata =
                                getFileFromServer( msg.filename, sender );
                            if ( filedata == null ) {
                                continue;
                            }
                            overwriteNewFile( directory + msg.filename,
                                filedata );
                            break;
                        }
                    } else if ( checkShardFilename( msg.filename ) ) {
                        byte[] data =
                            readBytesFromFile( directory + msg.filename );
                        if ( data != null ) {
                            boolean corrupt = checkShardForCorruption( data );
                            if ( !corrupt ) {
                                continue;
                            }
                        }
                        // Get all shards you can, and try to reconstruct the
                        // shard you need from it.
                        byte[][] shards = new byte[Constants.TOTAL_SHARDS][];
                        int index = -1;
                        for ( String server : msg.servers ) {
                            index++;
                            if ( server.equals( "-1" )
                                 || server.split( ":" ).length != 2 ) {
                                //System.out.println("Server isn't an
                                // address: " + server);
                                continue;
                            }
                            TCPSender sender =
                                Client.getTCPSender( tcpConnections, server );
                            if ( sender == null ) {
                                continue;
                            }
                            String shardname =
                                msg.filename.split( "_shard" )[0] + "_shard"
                                + String.valueOf( index );
                            byte[] filedata =
                                getFileFromServer( shardname, sender );
                            if ( filedata == null ) {
                                continue;
                            }
                            shards[index] = filedata;
                        }
                        byte[][] correctedShards =
                            decodeMissingShards( shards );
                        if ( correctedShards == null ) {
                            continue;
                        }
                        int sequence = Integer.valueOf(
                            msg.filename.split( "_shard" )[0].split(
                                "_chunk" )[1] );
                        int shardnumber = Integer.valueOf(
                            msg.filename.split( "_chunk" )[1].split(
                                "_shard" )[1] );
                        byte[] shardFileArray =
                            readyShardForStorage( sequence, shardnumber, 0,
                                correctedShards[shardnumber] );
                        overwriteNewFile( directory + msg.filename,
                            shardFileArray );
                    } else {
                        continue;
                    }
                }
            } catch ( Exception e ) {
                System.out.println(
                    "FileDistributionService run Exception: There was a "
                    + "problem processing the event." );
                e.printStackTrace();
            }
        }
    }
}
