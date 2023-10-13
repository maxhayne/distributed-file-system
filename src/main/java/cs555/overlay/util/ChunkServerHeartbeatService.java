package cs555.overlay.util;
import cs555.overlay.node.ChunkServer;
import cs555.overlay.wireformats.ChunkServerReportsFileCorruption;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;
import cs555.overlay.transport.ControllerConnection;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.FileMetadata;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class ChunkServerHeartbeatService extends TimerTask {

	private final ChunkServer chunkServer;
	private final Map<String,FileMetadata> majorFiles;
	private final Map<String,FileMetadata> minorFiles;
	private int heartbeatNumber;

	public ChunkServerHeartbeatService( ChunkServer chunkServer ) {
		this.chunkServer = chunkServer;
		this.majorFiles = new HashMap<String,FileMetadata>();
		this.minorFiles = new HashMap<String,FileMetadata>();
		this.heartbeatNumber = 1;
	}

	private byte[] majorHeartbeat() throws NoSuchAlgorithmException, IOException {
		majorFiles.clear();
		String[] files = chunkServer.getFileService().listFiles();
		for ( String filename : files ) {
			boolean shard = FileDistributionService.checkShardFilename( filename );
			if ( !shard ) { // then it's a chunk
				byte[] data = null;;
				Vector<Integer> errors;
				int numberOfErrors;
				int[] errorsarray = null;
				data = chunkServer.getFileService().readBytesFromFile( directory + filename );
				errors = FileDistributionService.checkChunkForCorruption( data );
				if ( data.length > 65720 ) {
					chunkServer.getFileService().truncateFile( directory + filename,65720 );
				}
				if ( !errors.isEmpty() ) {
					errorsarray = new int[errors.size()];
					for (int i = 0; i < errors.size(); i++)
						errorsarray[i] = errors.elementAt(i);
					numberOfErrors = errorsarray.length;
				} else {
					numberOfErrors = 0;
				}
				if ( numberOfErrors == 0 ) {
					ByteBuffer buffer = ByteBuffer.wrap( data );
					majorFiles.put( filename, new FileMetadata( filename, buffer.getInt(28) ) ); // skip first hash, read version
				} else {
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption( chunkServer.getIdentifier(), filename, errorsarray );
					chunkServer.getFileService().addToQueue(event); // Send message to Controller about corruption
				}
			} else { // This is a shard
				byte[] data = chunkServer.getFileService().readBytesFromFile(directory+filename);
				boolean corrupt = FileDistributionService.checkShardForCorruption(data);
				if (data.length > 10994) { chunkServer.getFileService().truncateFile(directory+filename,10994); }
				if (corrupt) {
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(chunkServer.getIdentifier(),filename,null);
					chunkServer.getFileService().addToQueue(event); // Send message to Controller about corruption
				} else {
					majorFiles.put(filename, new FileMetadata(filename));
				}
			}
		}

		// Once done with the loop, 'files' should be full of the list of files stored
		// in the node. Create message out of contents of 'files' and send to controller.
		int newfiles = majorFiles.size();
		String[] concattednames = null;
		if (newfiles > 0) {
			concattednames = new String[newfiles];
			int i = 0;
			for (Map.Entry<String,FileMetadata> mapping : majorFiles.entrySet()) {
				concattednames[i] = mapping.getValue().filename + "," + mapping.getValue().version;
				i += 1;
			}
		}
		int totalchunks = majorFiles.size();
		long freespace = chunkServer.getFileService().getUsableSpace();
		ChunkServerSendsHeartbeat heartbeat = new ChunkServerSendsHeartbeat( chunkServer.getIdentifier(),
			1, totalchunks, freespace, concattednames );
		byte[] bytes = null;;
		try {
			bytes = heartbeat.getBytes();
		} catch (IOException ioe) {
			System.out.println("ChunkServerHeartbeatService error: " + ioe);
			bytes = null;
		}
		return bytes;
	}

	// Actually need a majorMap that is cleared and populated on a majorHeartbeat, and a minorMap that 
	// is cleared and populated on a minorHeartbeat. During a minorHeartbeat, the minorMap is cleared,
	// and populated with any files that are not already present in the majorMap. Whatever is in the
	// minorMap at the end of the looping is sent to the controller, and then added to the majorMap.
	private byte[] minorHeartbeat() throws NoSuchAlgorithmException, IOException {
		minorFiles.clear();
		String[] filenames = chunkServer.getFileService().listFiles();
		for (String filename : filenames) {
			if (majorFiles.containsKey(filename))
				continue;
			boolean chunk = FileDistributionService.checkChunkFilename(filename);
			boolean shard = FileDistributionService.checkShardFilename(filename);
			if (chunk) {
				byte[] data = null;;
				Vector<Integer> errors;
				int numberOfErrors;
				int[] errorsarray = null;
				data = chunkServer.getFileService().readBytesFromFile(directory+filename);
				errors = FileDistributionService.checkChunkForCorruption(data);
				if (data.length > 65720) {
					chunkServer.getFileService().truncateFile(directory+filename,65720);
				}
				//System.out.println(data.length + " " + errors.size());
				if ( !errors.isEmpty() ) {
					errorsarray = new int[errors.size()];
					for (int i = 0; i < errors.size(); i++) {
						errorsarray[i] = errors.elementAt(i);
						//System.out.println(errorsarray[i]);
					}
					numberOfErrors = errorsarray.length;
				} else {
					numberOfErrors = 0;
				}
				if (numberOfErrors == 0) {
					ByteBuffer buffer = ByteBuffer.wrap(data);
					minorFiles.put(filename, new FileMetadata(filename,buffer.getInt(28)));
				} else {
					//System.out.println("Trying to add an event to the fileService.");
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(chunkServer.getIdentifier(),filename,errorsarray);
					chunkServer.getFileService().addToQueue(event);
				}
			} else if (shard) { // This is a shard
				byte[] data = chunkServer.getFileService().readBytesFromFile(directory+filename);
				boolean corrupt = FileDistributionService.checkShardForCorruption(data);
				if (data.length > 10994) { chunkServer.getFileService().truncateFile(directory+filename,10994); }
				if (corrupt) {
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(chunkServer.getIdentifier(),filename,null);
					chunkServer.getFileService().addToQueue(event); // Send message to Controller about corruption
				} else {
					minorFiles.put(filename, new FileMetadata(filename));
				}
			}
		}

		// Once done with the loop, 'files' should be full of the list of files stored
		// in the node. Create message out of contents of 'files' and send to controller.
		int newfiles = minorFiles.size();
		String[] concattednames = null;
		if (newfiles > 0) {
			concattednames = new String[newfiles];
			int i = 0;
			for (Map.Entry<String,FileMetadata> mapping : minorFiles.entrySet()) {
				concattednames[i] = mapping.getValue().filename + "," + mapping.getValue().version;
				i += 1;
			}
		}
		// Need to calculate total chunks and free space
		majorFiles.putAll(minorFiles);
		int totalchunks = majorFiles.size();
		long freespace = chunkServer.getFileService().getUsableSpace();
		ChunkServerSendsHeartbeat heartbeat = new ChunkServerSendsHeartbeat( chunkServer.getIdentifier(),
			0, totalchunks, freespace, concattednames );
		byte[] bytes = null;
		try {
			bytes = heartbeat.getBytes();
		} catch (IOException ioe) {
			System.out.println("ChunkServerHeartbeatService error: " + ioe);
			bytes = null;
		}
		return bytes;
	}

	public void run() {
		try {
			byte[] heartbeat = heartbeatNumber%10 == 0 ? majorHeartbeat() : minorHeartbeat();
			chunkServer.getControllerConnection().getSender().sendData( heartbeat );
			System.out.println( "Heartbeat " + heartbeatNumber + " has been sent to the Controller at " + new Date() );
			heartbeatNumber += 1;
		} catch ( NoSuchAlgorithmException nsae ) {
			System.err.println( "ChunkServerHeartbeatService run: NoSuchAlgorithmException. " + nsae.getMessage() );
		} catch( IOException ioe ) {
			System.err.println( "ChunkServerHeartbeatService run: Unable to send heartbeat to Controller. " + ioe.getMessage() );
		}
	}
}