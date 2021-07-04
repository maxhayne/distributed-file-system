package cs555.overlay.util;
import cs555.overlay.wireformats.ChunkServerReportsFileCorruption;
import cs555.overlay.wireformats.ChunkServerSendsHeartbeat;
import cs555.overlay.transport.ControllerConnection;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.FileMetadata;
import java.security.NoSuchAlgorithmException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map;

public class ChunkServerHeartbeatService extends TimerTask {

	private String directorystring;
	private ControllerConnection connection;
	private Map<String,FileMetadata> majorFiles;
	private Map<String,FileMetadata> minorFiles;
	private int heartbeatnumber;
	private FileDistributionService fileservice; 

	public ChunkServerHeartbeatService(String directory, ControllerConnection connection, FileDistributionService fileservice) {
		this.directorystring = directory;
		this.connection = connection;
		this.heartbeatnumber = 1;
		this.majorFiles = new TreeMap<String,FileMetadata>();
		this.minorFiles = new TreeMap<String,FileMetadata>();
		this.fileservice = fileservice;
	}

	private boolean checkChunkFilename(String filename) {
		boolean matches = filename.matches(".*_chunk[0-9]*$");
		String[] split = filename.split("_chunk");
		if (matches && split.length == 2) {
			return true;
		}
		return false;
	}

	private boolean checkShardFilename(String filename) {
		boolean matches = filename.matches(".*_chunk[0-9]*_shard[0-8]$");
		String[] split1 = filename.split("_chunk");
		String[] split2 = filename.split("_shard");
		if (matches && split1.length == 2 && split2.length == 2) {
			return true;
		}
		return false;
	}

	private byte[] majorHeartbeat() throws NoSuchAlgorithmException {
		majorFiles.clear();
		String[] files = fileservice.listFiles();
		for (String filename : files) {
			boolean shard = checkShardFilename(filename);
			if (!shard) { // then it's a chunk
				byte[] data = null;;
				Vector<Integer> errors;
				int numberOfErrors;
				int[] errorsarray = null;
				data = fileservice.readBytesFromFile(directorystring+filename);
				errors = fileservice.checkChunkForCorruption(data);
				if (data.length > 65720) { fileservice.truncateFile(directorystring+filename,65720); }
				if (errors.size() > 0) {
					errorsarray = new int[errors.size()];
					for (int i = 0; i < errors.size(); i++)
						errorsarray[i] = errors.elementAt(i);
					numberOfErrors = errorsarray.length;
				} else {
					numberOfErrors = 0;
				}
				if (numberOfErrors == 0) {
					ByteBuffer buffer = ByteBuffer.wrap(data);
					majorFiles.put(filename, new FileMetadata(filename,buffer.getInt(28))); // skip first hash, read version
				} else {
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(connection.getIdentifier(),filename,errorsarray);
					fileservice.addToQueue(event); // Send message to Controller about corruption
				}
			} else { // This is a shard
				byte[] data = fileservice.readBytesFromFile(directorystring+filename);
				boolean corrupt = fileservice.checkShardForCorruption(data);
				if (data.length > 10994) { fileservice.truncateFile(directorystring+filename,10994); }
				if (corrupt) {
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(connection.getIdentifier(),filename,null);
					fileservice.addToQueue(event); // Send message to Controller about corruption
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
		long freespace = fileservice.getUsableSpace();
		ChunkServerSendsHeartbeat heartbeat = new ChunkServerSendsHeartbeat(1,totalchunks,freespace,concattednames);
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
	private byte[] minorHeartbeat() throws NoSuchAlgorithmException {
		minorFiles.clear();
		String[] filenames = fileservice.listFiles();
		for (String filename : filenames) {
			if (majorFiles.containsKey(filename))
				continue;
			boolean chunk = checkChunkFilename(filename);
			boolean shard = checkShardFilename(filename);
			if (chunk) {
				byte[] data = null;;
				Vector<Integer> errors;
				int numberOfErrors;
				int[] errorsarray = null;
				data = fileservice.readBytesFromFile(directorystring+filename);
				errors = fileservice.checkChunkForCorruption(data);
				if (data.length > 65720) {
					fileservice.truncateFile(directorystring+filename,65720);
				}
				//System.out.println(data.length + " " + errors.size());
				if (errors.size() > 0) {
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
					//System.out.println("Trying to add an event to the fileservice.");
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(connection.getIdentifier(),filename,errorsarray);
					fileservice.addToQueue(event);
				}
			} else if (shard) { // This is a shard
				byte[] data = fileservice.readBytesFromFile(directorystring+filename);
				boolean corrupt = fileservice.checkShardForCorruption(data);
				if (data.length > 10994) { fileservice.truncateFile(directorystring+filename,10994); }
				if (corrupt) {
					ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(connection.getIdentifier(),filename,null);
					fileservice.addToQueue(event); // Send message to Controller about corruption
				} else {
					majorFiles.put(filename, new FileMetadata(filename));
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
		long freespace = fileservice.getUsableSpace();
		ChunkServerSendsHeartbeat heartbeat = new ChunkServerSendsHeartbeat(0,totalchunks,freespace,concattednames);
		byte[] bytes = null;;
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
			byte[] heartbeat = heartbeatnumber%10 == 0 ? majorHeartbeat() : minorHeartbeat();
			System.out.println("Heartbeat is running. heartbeatnumber: " + heartbeatnumber + ". Current time: " + System.currentTimeMillis() + ".");
			connection.addToSendQueue(heartbeat);
			heartbeatnumber += 1;
		} catch (NoSuchAlgorithmException nsae) {
			System.out.println("ChunkServerHeartbeatService run: NoSuchAlgorithmException.");
		}
	}
}