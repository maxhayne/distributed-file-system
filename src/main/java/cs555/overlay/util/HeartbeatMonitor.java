package cs555.overlay.util;

import cs555.overlay.wireformats.ControllerRequestsFileAcquire;
import cs555.overlay.wireformats.ControllerRequestsFileDelete;
import cs555.overlay.transport.ChunkServerConnectionCache;
import cs555.overlay.transport.ChunkServerConnection;
import cs555.overlay.transport.TCPSender;
import cs555.overlay.node.ChunkServer;

import java.lang.StringBuilder;
import java.net.UnknownHostException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.net.Socket;

public class HeartbeatMonitor extends TimerTask {

    private final ChunkServerConnectionCache connectionCache;
    private final Map<Integer, ChunkServerConnection> chunkCache;
    private final DistributedFileCache idealState;
    private final DistributedFileCache reportedState;

    public HeartbeatMonitor( ChunkServerConnectionCache connectionCache,
        Map<Integer, ChunkServerConnection> chunkCache,
        DistributedFileCache idealState, DistributedFileCache reportedState ) {
        this.connectionCache = connectionCache;
        this.chunkCache = chunkCache;
        this.idealState = idealState;
        this.reportedState = reportedState;
    }

    private boolean checkChunkFilename( String filename ) {
        boolean matches = filename.matches( ".*_chunk[0-9]*$" );
        String[] split = filename.split( "_chunk" );
        return matches && split.length == 2;
    }

    private boolean checkShardFilename( String filename ) {
        boolean matches = filename.matches( ".*_chunk[0-9]*_shard[0-8]$" );
        String[] split1 = filename.split( "_chunk" );
        String[] split2 = filename.split( "_shard" );
        return matches && split1.length == 2 && split2.length == 2;
    }

    // If the version is -1, it is a shard, not a chunk!
    // Next step will be to determine if the latest heartbeat is valid,
    // meaning it has the right type, and the timestamp is within 15 seconds of
    // the heartbeat.

    public synchronized void run() {
        Vector<Integer> toRemove = new Vector<Integer>();

        synchronized ( chunkCache ) { // lock the chunkCache
            if ( chunkCache.isEmpty() ) {
                return; // no information to report
            }

            System.out.println( "\nHeartbeat Monitor:" );
            long now = System.currentTimeMillis();
            for ( ChunkServerConnection connection : chunkCache.values() ) {
                System.out.print( connection.print() );
                HeartbeatInfo heartbeatInfo = connection.getHeartbeatInfo();


                // Copy over HeartbeatInfo
                ArrayList<FileMetadata> newFiles;
                long lastMajorHeartbeat;
                long lastMinorHeartbeat;
                synchronized ( heartbeatInfo ) {
                    newFiles = heartbeatInfo.getFiles();
                    lastMajorHeartbeat = heartbeatInfo.getLastMajorHeartbeat();
                    lastMinorHeartbeat = heartbeatInfo.getLastMinorHeartbeat();
                }

                // Print list of new files
                if ( !newFiles.isEmpty() ) {
                    StringBuilder sb = new StringBuilder();
                    for ( FileMetadata newFile : newFiles ) {
                        sb.append( newFile.filename ).append( ", " );
                    }
                    sb.delete( sb.length() - 2, sb.length() );
                    System.out.println( "[ " + sb.toString() + " ]" );
                }

                // Determine which type of heartbeat was most recent
                int beatType;
                long lastHeartbeat;
                if ( now - lastMajorHeartbeat < now - lastMinorHeartbeat ) {
                    lastHeartbeat = now - lastMajorHeartbeat;
                    beatType = 1;
                } else {
                    lastHeartbeat = now - lastMinorHeartbeat;
                    beatType = 0;
                }

                // Do some logic on the reportedState DistributedFileCache,
                // assuming this is a healthy heartbeat
                if ( lastHeartbeat < Constants.HEARTRATE ) {
                    Vector<Chunk> newChunks = new Vector<Chunk>();
                    Vector<Shard> newShards = new Vector<Shard>();
                    for ( FileMetadata meta : newFiles ) {
                        if ( checkShardFilename( meta.filename ) ) { // shard
                            String[] chunkSplit =
                                meta.filename.split( "_chunk" );
                            String[] shardSplit =
                                meta.filename.split( "_shard" );
                            int sequence = Integer.parseInt(
                                chunkSplit[1].split( "_shard" )[0] );
                            int fragment = Integer.parseInt( shardSplit[1] );
                            Shard newShard =
                                new Shard( chunkSplit[0], sequence, fragment,
                                    meta.version, meta.timestamp,
                                    connection.getIdentifier(), false );
                            newShards.add( newShard );
                        } else { // chunk
                            String[] chunkSplit =
                                meta.filename.split( "_chunk" );
                            int sequence = Integer.parseInt( chunkSplit[1] );
                            Chunk newChunk = new Chunk( chunkSplit[0], sequence,
                                meta.version, meta.timestamp,
                                connection.getIdentifier(), false );
                            newChunks.add( newChunk );
                        }
                    }
                    if ( beatType == 0 ) { // Minor heartbeat
                        // Add all the new files...
                        for ( Chunk chunk : newChunks ) {
                            reportedState.addChunk( chunk );
                        }
                        for ( Shard shard : newShards ) {
                            reportedState.addShard( shard );
                        }
                    } else { // Major heartbeat
                        // Remove all of this node's old files from the
                        // filecache
                        reportedState.removeAllFilesAtServer(
                            connection.getIdentifier() );
                        // and add all the new files...
                        for ( Chunk chunk : newChunks ) {
                            reportedState.addChunk( chunk );
                        }
                        for ( Shard shard : newShards ) {
                            reportedState.addShard( shard );
                        }
                    }
                    newChunks.clear();
                    newShards.clear();
                }

                // Now need to handle some logic having to do with whether a
                // ChunkServer is alive
                int unhealthy = 0;
                if ( lastMajorHeartbeat != -1 && now - lastMajorHeartbeat > (
                    Constants.HEARTRATE * 11 ) ) {
                    unhealthy += 1;
                }
                if ( lastMinorHeartbeat != -1 && now - lastMinorHeartbeat > (
                    Constants.HEARTRATE * 2 ) ) {
                    unhealthy += 1 + ( int ) (
                        ( now - lastMinorHeartbeat - ( Constants.HEARTRATE
                                                       * 2 ) )
                        / Constants.HEARTRATE ); // extra unhealthy for more
                }
                // missed minor heartbeats
                if ( now - connection.getStartTime() > ( Constants.HEARTRATE
                                                         * 2 )
                     && lastMinorHeartbeat == -1 ) {
                    unhealthy += 1;
                }
                if ( now - connection.getStartTime() > Constants.HEARTRATE
                     && lastMajorHeartbeat == -1 ) {
                    unhealthy += 1;
                }

                // Add a 'poke' message to send to the ChunkServer, and on
                // the next heartbeat, add to 'unhealthy' the discrepancy
                // between the 'pokes' and the 'poke replies'.

                // Can have total of 4 strikes the server, 2 or more should
                // be considered unhealthy
                if ( unhealthy >= 2 ) {
                    connection.incrementUnhealthy();
                } else {
                    connection.decrementUnhealthy();
                }

                // If this node has failed 3 times in a row, remove it from
                // the cache of ChunkServers
                if ( connection.getUnhealthy() > 3 ) {
                    toRemove.add( connection.getIdentifier() );
                }
            }

            // Prune these data structures to remove stragglers
            idealState.prune();
            reportedState.prune();

            // Can use this to try to replace files that have somehow
            // vanished from their correct nodes.
            try {
                //System.out.println("Files in 'idealState' that aren't in
                // 'reportedState':");
                Vector<String> diffs1 = idealState.differences( reportedState );
                for ( String diff : diffs1 ) {
                    // filetype,basename,sequence,version,serveridentifier,
                    // createdTime
                    String[] parts = diff.split( "," );
                    // Need to send to this server information about the
                    // filename of what it should acquire, along with the
                    // list of servers
                    if ( System.currentTimeMillis() - Long.parseLong( parts[5] )
                         < Constants.HEARTRATE ) {
                        continue; // If the chunk is new, might just not have
                    }
                    // been stored yet.
                    String serverInfo =
                        connectionCache.getChunkServerServerAddress(
                            Integer.parseInt( parts[4] ) );
                    if ( serverInfo.equals( "" ) ) {
                        continue;
                    }
                    String filename;
                    String storageInfo =
                        connectionCache.getChunkStorageInfo( parts[1],
                            Integer.parseInt( parts[2] ) );
                    String[] servers;
                    if ( parts[0].equals( "chunk" ) ) { // It is a chunk
                        filename = parts[1] + "_chunk" + parts[2];
                        servers =
                            storageInfo.split( "\\|", -1 )[0].split( "," );
                        // Remove from servers the ChunkServer we are sending
                        // this request to
                        String[] serversWithoutSelf;
                        int total = 0;
                        for ( String server : servers ) {
                            if ( !server.equals( serverInfo ) ) {
                                total++;
                            }
                        }
                        if ( total == 0 ) {
                            continue;
                        }
                        serversWithoutSelf = new String[total];
                        int index = 0;
                        for ( String server : servers ) {
                            if ( !server.equals( serverInfo ) ) {
                                serversWithoutSelf[index] = server;
                                index++;
                            }
                        }
                        servers = serversWithoutSelf;
                    } else { // It is a shard
                        filename = parts[1] + "_chunk" + parts[2] + "_shard"
                                   + parts[3];
                        servers =
                            storageInfo.split( "\\|", -1 )[1].split( "," );
                        if ( servers.length == 1 && servers[0].equals( "" ) ) {
                            continue;
                        }
                    }
                    ControllerRequestsFileAcquire acquire =
                        new ControllerRequestsFileAcquire( filename, servers );
                    connectionCache.sendToChunkServer(
                        Integer.parseInt( parts[4] ), acquire.getBytes() );
                }
            } catch ( Exception e ) {
            } // All of this is best effort, so don't let anything derail the
            // HeartbeatMonitor

			/*
			// Try to delete files from Chunk Servers that shouldn't be there.
			try {
				// Can use this one to delete files that should no longer be
				where they are.
				//System.out.println("Files in 'reportedState' that aren't in
				'idealState':");
				Vector<String> diffs2 = reportedState.differences(idealState);
				for (String diff : diffs2) {
					// filetype,basename,sequence,shardnumber,
					serveridentifier,createdTime
					//System.out.println(diff);
					String[] parts = diff.split(",");
					ControllerRequestsFileDelete msg;
					if (parts[0].equals("shard")) {
						String filename = parts[1] + "_chunk" + parts[2] +
						"_shard" + parts[3];
						msg = new ControllerRequestsFileDelete(filename);
					} else {
						String filename = parts[1] + "_chunk" + parts[2];
						msg = new ControllerRequestsFileDelete(filename);
					}
					int identifier = Integer.valueOf(parts[4]);
					try {
						String address = connectionCache
						.getChunkServerServerAddress(identifier);
						if (address.equals("")) continue;
						String hostname = address.split(":")[0];
						int port = Integer.valueOf(address.split(":")[1]);
						Socket connection = new Socket(hostname,port);
						TCPSender sender = new TCPSender(connection);
						sender.sendData(msg.getBytes());
						connection.close();
						sender = null;
						// Remove the file from reportedState
						if (parts[0].equals("shard")) {
							reportedState.removeShard(new Shard(parts[1],
							Integer.valueOf(parts[2]),Integer.valueOf
							(parts[3]),Integer.valueOf(parts[4]),false));
						} else {
							reportedState.removeChunk(new Chunk(parts[1],
							Integer.valueOf(parts[2]),Integer.valueOf
							(parts[3]),Integer.valueOf(parts[4]),false));
						}
					} catch (Exception e) {
						// This is best effort.
					}
				}
			} catch (Exception e) {
				// All of this is best effort, so don't let anything derail
				the HeartbeatMonitor
			}
			*/

            System.out.println();
        }
        for ( Integer i : toRemove ) {
            connectionCache.deregister( i );
        }
        toRemove.clear();
    }
}