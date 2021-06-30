package cs555.overlay.node;
import cs555.overlay.util.ChunkServerHeartbeatService;
import cs555.overlay.transport.ControllerConnection;
import cs555.overlay.transport.TCPReceiverThread;
import java.util.concurrent.ThreadLocalRandom;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import java.security.MessageDigest;
import cs555.overlay.wireformats.*;
import java.io.RandomAccessFile;
import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.util.TimerTask;
import erasure.ReedSolomon;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Vector;
import java.util.Timer;
import java.net.Socket;
import java.lang.Math;
import java.io.File;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.BufferUnderflowException;

public class ChunkServer {

	public static final int DATA_SHARDS = 6; 
	public static final int PARITY_SHARDS = 3; 
	public static final int TOTAL_SHARDS = 9;
	public static final int BYTES_IN_INT = 4;
	public static final int BYTES_IN_LONG = 8;
	public static final String CONTROLLER_HOSTNAME = "192.168.68.65";
	public static final int CONTROLLER_PORT = 50000;

	// Function for generating hash
	public static byte[] SHA1FromBytes(byte[] data) throws NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance("SHA1");
		byte[] hash = digest.digest(data);
		return hash; 
	}

	public static byte[] getNextChunkFromFile(String filename, int sequence) {
		int position = sequence*65536;
		try (RandomAccessFile file = new RandomAccessFile(filename, "r");
      		FileChannel channel = file.getChannel();
      		FileLock lock = channel.lock(position, 65536, true)) {
			if (position > channel.size())
				return null;
			byte[] data = new byte[65536]; 
			ByteBuffer buffer = ByteBuffer.wrap(data);
			int read = 1;
			while (buffer.hasRemaining() && read > 0) {
				read = channel.read(buffer,position);
				position += read;
			}
        	return data;
		} catch (IOException ioe) {
			return null;
		}
	}

	public static void truncateFile(String filename, long size) {
		try (RandomAccessFile file = new RandomAccessFile(filename, "rw");
      		FileChannel channel = file.getChannel();
      		FileLock lock = channel.lock()) {
			channel.truncate(size);
		} catch (IOException ioe) {
			// Nothing to do here...
		}
	}

	// Takes chunk data, combines with metadata and hashes, basically prepares it for 
	// writing to a file.
	public static byte[] readyChunkForStorage(int sequence, int version, byte[] chunkArray) throws NoSuchAlgorithmException {
		int chunkArrayRemaining = chunkArray.length;
		byte[] chunkToFileArray = new byte[65720]; // total size of stored chunk
		byte[] sliceArray = new byte[8195];
		ByteBuffer chunkToFileBuffer = ByteBuffer.wrap(chunkToFileArray);
		ByteBuffer sliceBuffer = ByteBuffer.wrap(sliceArray);
		sliceBuffer.putInt(0); // padding
		sliceBuffer.putInt(sequence);
		sliceBuffer.putInt(version);
		sliceBuffer.putInt(chunkArrayRemaining);
		sliceBuffer.putLong(System.currentTimeMillis());
		int position = 0;
		if (chunkArrayRemaining >= 8195-24) {
			sliceBuffer.put(chunkArray,position,8195-24);
			chunkArrayRemaining -= (8195-24);
			position += (8195-24);
		} else {
			sliceBuffer.put(chunkArray,0,chunkArrayRemaining);
			chunkArrayRemaining = 0;
		}
		byte[] hash = SHA1FromBytes(sliceArray);
		chunkToFileBuffer.put(hash);
		chunkToFileBuffer.put(sliceArray);
		sliceBuffer.clear();
		Arrays.fill(sliceArray,(byte)0);
		for (int i = 0; i < 7; i++) {
			if (chunkArrayRemaining == 0) {
				hash = SHA1FromBytes(sliceArray);
			} else if (chunkArrayRemaining < 8195) {
				sliceBuffer.put(chunkArray,position,chunkArrayRemaining);
				chunkArrayRemaining = 0;
				hash = SHA1FromBytes(sliceArray);
			} else {
				sliceBuffer.put(chunkArray,position,8195);
				chunkArrayRemaining -= 8195;
				position += 8195;
				hash = SHA1FromBytes(sliceArray);
			}
			chunkToFileBuffer.put(hash);
			chunkToFileBuffer.put(sliceArray);
			sliceBuffer.clear();
			Arrays.fill(sliceArray,(byte)0);
		}
		return chunkToFileArray;
	}

	// Read any file and return a byte[] of the data
	public static byte[] readBytesFromFile(String filename) {
		try (RandomAccessFile file = new RandomAccessFile(filename, "r");
      		FileChannel channel = file.getChannel();
      		FileLock lock = channel.lock(0, Long.MAX_VALUE, true)) {
			byte[] data = new byte[(int)channel.size()]; 
			ByteBuffer buffer = ByteBuffer.wrap(data);
			int read = 1;	
			int position = 0;
			while (buffer.hasRemaining() && read > 0) {
				read = channel.read(buffer,position);
				position += read;
			}
        	return data;
		} catch (IOException ioe) {
			return null;
		}
	}

	// Check chunk for errors and return integer array containing slice numbers
	public static Vector<Integer> checkChunkForCorruption(byte[] chunkArray) throws NoSuchAlgorithmException {
		ByteBuffer chunk = ByteBuffer.wrap(chunkArray);
		Vector<Integer> corrupt = new Vector<Integer>();
		for (int i = 0; i < 8; i++)
			corrupt.add(i);
		byte[] hash = new byte[20];
		byte[] slice = new byte[8195];
		try {
			for (int i = 0; i < 8; i++) {
				chunk.get(hash);
				chunk.get(slice);
				byte[] computedHash = SHA1FromBytes(slice);
				if (Arrays.equals(hash,computedHash)) {
					corrupt.removeElement(i);
				}
				Arrays.fill(hash,(byte)0);
				Arrays.fill(slice,(byte)0);
			}
		} catch (BufferUnderflowException bue) {
			// The array wasn't the correct length for a chunk
		}
		// The array could pass all of the tests and still be corrupt, if any information
		// was added to the end of the file storing the chunk. Correct length of the file
		// is 65720 bytes.
		return corrupt;
	}

	// Removes hashes from chunk
	public static byte[] removeHashesFromChunk(byte[] chunkArray) {
		ByteBuffer chunk = ByteBuffer.wrap(chunkArray);
		byte[] cleanedChunk = new byte[65560];
		for (int i = 0; i < 8; i++) {
			chunk.position(chunk.position()+20);
			chunk.get(cleanedChunk,i*8195,8195);
		}
		return cleanedChunk;
		// cleanedChunk will start like this:
		// Padding (0 int)
		// Sequence (int)
		// Version (int)
		// Total bytes of data in this chunk (int)
		// Timestamp (long)
	}

	// Removes metadata, and strips padding from end of array
	public static byte[] getDataFromChunk(byte[] chunkArray) {
		ByteBuffer chunk = ByteBuffer.wrap(chunkArray);
		int chunkLength = chunk.getInt(12);
		chunk.position(24);
		byte[] data = new byte[chunkLength];
		chunk.get(data);
		return data;
	}

	// Create file if it doesn't exist, if it does exist, return false
	public static boolean writeNewFile(String filename, byte[] data) {
		File tryFile = new File(filename);
		if (tryFile.isFile()) { 
    		return false;
		}
		tryFile = null;
		try (RandomAccessFile file = new RandomAccessFile(filename, "rw");
      		FileChannel channel = file.getChannel();
      		FileLock lock = channel.lock()) {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			while (buffer.hasRemaining()) {
				channel.write(buffer);
			}
			return true;
		} catch (IOException ioe) {
			return false;
		}
	}

	// Write new file, replace if it already exists.
	public static boolean overwriteNewFile(String filename, byte[] data) {
		try (RandomAccessFile file = new RandomAccessFile(filename, "rw");
      		FileChannel channel = file.getChannel();
      		FileLock lock = channel.lock()) {
			channel.truncate(0);
			ByteBuffer buffer = ByteBuffer.wrap(data);
			while (buffer.hasRemaining()) {
				channel.write(buffer);
			}
			return true;
		} catch (IOException ioe) {
			return false;
		}
	}

	// Replace slices with new slices
	public static boolean replaceSlices(String filename, int[] slices, byte[][] sliceData) {
		File tryFile = new File(filename);
		if (!tryFile.isFile()) {
    		return false; // can't replace slices for a file that doesn't exist
		}
		tryFile = null;
		try (RandomAccessFile file = new RandomAccessFile(filename, "rw");
      		FileChannel channel = file.getChannel();
      		FileLock lock = channel.lock()) {
			int numSlices = slices.length;
			for (int i = 0; i < numSlices; i++) {
				int position = 8195*slices[i];
				ByteBuffer newSlice = ByteBuffer.wrap(sliceData[i]);
				while (newSlice.hasRemaining()) {
					position += channel.write(newSlice,position);
				}
			}
			return true;
		} catch (IOException ioe) {
			return false;
		}
	}

	// Replace slices with new slices
	public static byte[][] getSlices(byte[] chunkArray, int[] slices) {
		int numSlices = slices.length;
		byte[][] sliceData = new byte[numSlices][8195];
		for (int i = 0; i < numSlices; i++) {
			ByteBuffer buffer = ByteBuffer.wrap(sliceData[i]);
			buffer.put(chunkArray,slices[i]*8195,8195);
		}
		return sliceData;
	}

	/*
	// Get chunk data (including metadata) from shard byte[][] array
	public static byte[] getChunkFromShards(byte[][] shardArray) {

	}
	*/
	
	public static void main(String[] args) throws Exception {
		// CHECK ARGUMENTS. NEED PATH AND PORT.
		if (args.length > 2 || args.length < 1)
			System.out.println("Only two command line arguments are accepted: the directory into which it will store files, and the (optional) port number on which the ChunkServer will operate.");
		String directoryString = args[0];
		File directory = new File(directoryString);
		if (!directory.isDirectory()) {
			System.out.println("Error: '" + directoryString + "' is not a valid directory. It either doesn't exist, or it isn't a directory.");
			return;
		}
		int accessPort = 0;
		if (args.length == 2) {
			try {
				accessPort = Integer.parseInt(args[1]);
				if (accessPort > 65535 || accessPort < 1024) {
					System.out.println("The port number must be between 1024 and 65535. An open port will be chosen automatically.");
					accessPort = 0;
				}
			} catch (NumberFormatException e) {
				System.out.println("Error: '" + args[0] + "' is not an integer. An open port will be chosen automatically.");
				accessPort = 0;
			}
		}
		if (!directoryString.endsWith("/"))
			directoryString += "/";
		System.out.println(directoryString);
		// END OF CHECKING ARGUMENTS

		// NOW NEED TO CONNECT TO CONTROLLER

		/*
		// Create a temporary file for testing purposes
		if (directoryString.charAt(directoryString.length()-1) != '/')
			directoryString += '/';
		String tempFileName = directoryString + "tempFile";
		File tempFile = new File(tempFileName);
		try {
			if (tempFile.createNewFile())
	        	System.out.println("File created: " + tempFile.getName());
	      	else
	        	System.out.println("File already exists.");
	    } catch(IOException e) {
	    	System.out.println("An error occurred.");
      		e.printStackTrace();
	    }
	    try {
		    FileWriter myWriter = new FileWriter(tempFileName);
		    for (int i = 0; i < 1000; i++) {
		    	for (int j = 0; j < 1000; j++)
		    		myWriter.write(ThreadLocalRandom.current().nextInt(0, 127 + 1)); // write random character
		    }
		    myWriter.close();
		    System.out.println("Successfully wrote to the file.");
		} catch(IOException e) {
			System.out.println("An error occurred.");
      		e.printStackTrace();
		}

		// Read a file
		byte[] chunkFileData = getNextChunkFromFile(tempFileName,0); // first chunk's data
		byte[] chunkPrepared = readyChunkForStorage(0,0,chunkFileData);
		Vector<Integer> corruptSlices = checkChunkForCorruption(chunkPrepared);
		System.out.println(chunkPrepared.length + " " + corruptSlices.size());
		String newChunk = tempFileName + "_chunk0";
		boolean isWritten = writeNewFile(newChunk,chunkPrepared);
		System.out.println("isWritten: " + isWritten);
		byte[] readChunk = readBytesFromFile(newChunk);
		corruptSlices = checkChunkForCorruption(readChunk);
		System.out.println(readChunk.length + " " + corruptSlices.size());
		if (corruptSlices.size() == 0) {
			byte[] retrievedData = getDataFromChunk(removeHashesFromChunk(readChunk));
			if (Arrays.equals(chunkFileData,retrievedData))
				System.out.println("They match.");
			else
				System.out.println("They DON'T match.");
		}

		int[] slices = {0,5,7};
		byte[][] sliceData = getSlices(chunkPrepared,slices);
		System.out.println(sliceData.length);

		boolean replacedSlices = replaceSlices(newChunk,slices,sliceData);
		if (replacedSlices) {
			System.out.println("Replaced the slices.");
			byte[] readNewestChunk = readBytesFromFile(newChunk);
			corruptSlices = checkChunkForCorruption(readChunk);
			System.out.println(readChunk.length + " " + corruptSlices.size());
			byte[] newestRetrievedData = getDataFromChunk(removeHashesFromChunk(readNewestChunk));
			if (Arrays.equals(chunkFileData,newestRetrievedData))
				System.out.println("They match.");
			else
				System.out.println("They DON'T match.");
		}

		int index = 0;
		byte[] loopChunk = new byte[8];
		while (loopChunk != null) {
			loopChunk = getNextChunkFromFile(tempFileName,index);
			if (loopChunk != null) {
				byte[] preparedChunk = readyChunkForStorage(index,0,chunkFileData);
				Vector<Integer> corrupt = checkChunkForCorruption(preparedChunk);
				if (corrupt.size() == 0) {
					String filename = tempFileName + "_chunk" + String.valueOf(index);
					boolean written = writeNewFile(filename,preparedChunk);
					System.out.println(filename + " " + written);
				}
			}
			index++;
		}
		*/
		
		/*
		try {


			// Try some ReedSolomon file encoding
			int fileSize = testRead.length;
			int storedSize = fileSize + BYTES_IN_INT;
			int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;
			int bufferSize = shardSize * DATA_SHARDS;
			byte[] allBytes = new byte[bufferSize];
			ByteBuffer allBytesBuffer = ByteBuffer.wrap(allBytes);
			allBytesBuffer.put(testRead);
			byte[][] shards = new byte[TOTAL_SHARDS][shardSize];
			for (int i = 0; i < DATA_SHARDS; i++) {
				System.arraycopy(allBytes,i*shardSize,shards[i],0,shardSize);
			}
			ByteBuffer shard0wrap = ByteBuffer.wrap(shards[0]);
			System.out.println("shard0 size: " + shard0wrap.getInt(0));
			ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS,PARITY_SHARDS);
			reedSolomon.encodeParity(shards,0,shardSize);
			System.out.println("Encoded Parity.");
			System.out.println(shards[0].length);

			// Now let's try decoding...
			byte[][] shardDecode = new byte[TOTAL_SHARDS][];
			boolean[] shardPresent = new boolean[TOTAL_SHARDS];
			int shardCount = 0;
			for (int i = 0; i < TOTAL_SHARDS; i++) {
				shardDecode[i] = Arrays.copyOf(shards[i],shards[i].length);
				shardPresent[i] = true;
				shardCount++;
			}
			if (Arrays.equals(shards[0],shardDecode[0]) && Arrays.equals(shards[1],shardDecode[1]) && Arrays.equals(shards[2],shardDecode[2]) && Arrays.equals(shards[3],shardDecode[3]) && Arrays.equals(shards[4],shardDecode[4]) && Arrays.equals(shards[5],shardDecode[5])) {
				System.out.println("Original shards match decoded shards.");
			} else {
				System.out.println("Original shards don't match decoded shards.");
			}
			if (shardCount < DATA_SHARDS) {
				System.out.println("Can't decode shards.");
			}

			for (int i = 0; i < TOTAL_SHARDS; i++) {
				if (!shardPresent[i]) {
					shardDecode[i] = new byte[shardSize];
				}
			}

			reedSolomon.decodeMissing(shardDecode,shardPresent,0,shardSize);
			byte[] decodedChunk = new byte[shardSize*DATA_SHARDS];
			System.out.println("shardSize*DATA_SHARDS: " + shardSize*DATA_SHARDS);
			for (int i = 0; i < DATA_SHARDS; i++) {
            	System.arraycopy(shardDecode[i], 0, decodedChunk, shardSize * i, shardSize);
        	}
        	ByteBuffer temp = ByteBuffer.wrap(decodedChunk);
        	System.out.println("Sequence Number: " + temp.getInt());
        	System.out.println("Version Number: " + temp.getInt());
        	System.out.println("Size: " + temp.getInt());
        	System.out.println("Timestamp: " + temp.getLong());
        	System.out.println("Current Time: " + System.currentTimeMillis());
        	byte[] correctedDecode = Arrays.copyOf(decodedChunk,temp.getInt(8)+20+160);
        	System.out.println("decodedChunk.length: " + decodedChunk.length);
        	System.out.println("correctedDecode.length: " + correctedDecode.length);

        	if (Arrays.equals(correctedDecode,testRead)) {
				System.out.println("correctedDecode matches testRead.");
			} else {
				System.out.println("correctedDecode doesn't match testRead.");
			}

			// Check free space
			File testSpace = new File("/");
			System.out.println(testSpace.getFreeSpace());

		} catch (Exception e) {
			System.out.println(e);
			return;
		}
		// 8192 Bytes in 8KB
		// 65536 Bytes in 64KB
		try {
			byte[] hash = SHA1FromBytes(chunk.array());
			System.out.println(hash);
			System.out.println(hash.length);
		} catch (NoSuchAlgorithmException e) {
			System.out.println("There was a problem hashing the chunk.");
			return;
		}

		// Erasure Coding start
		int fileSize = (int) tempFile.length();
		int storedSize = fileSize + BYTES_IN_INT;
		int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;
		int bufferSize = shardSize * DATA_SHARDS;
		byte[] allBytes = new byte[bufferSize];
		// Need to copy the file into this byte array
		*/

		// Let's try to connect to the Controller as a chunk server
		try {
			InetAddress inetAddress = InetAddress.getLocalHost(); // grabbing local address to pass in registration
			String host = inetAddress.getHostAddress().toString();
			ServerSocket server = new ServerSocket(0,32,inetAddress);
			System.out.println("[" + host + ":" + server.getLocalPort() + "]");
			ControllerConnection controllerConnection = new ControllerConnection(CONTROLLER_HOSTNAME,CONTROLLER_PORT,server,directoryString);
			controllerConnection.start(); // This creates a server, receiver, filedistributionservice
			ChunkServerSendsRegistration registration = new ChunkServerSendsRegistration(host,server.getLocalPort());
			// This, if successful, will start the heartbeat service in the ControllerConnection
			controllerConnection.addToSendQueue(registration.getBytes());
		} catch (Exception e) {
			System.err.println("There was a problem setting up a socket connection to the controller.");
			System.err.println(e);
			return;
		}
	}
} 

/*
// Try to read a chunk
			RandomAccessFile data = new RandomAccessFile(tempFile, "r");
			int readValue = data.readFully(readChunk.array());
			slice.putInt(0); // Sequence Number of File (which chunk is it?)
			slice.putShort((short)0); // Version Number of File (which version is this?)
			slice.putShort((short)readValue); // Size of data in chunk (is it full?)
			slice.putLong(System.currentTimeMillis()); // add timestame to version
			int position = 0;
			slice.put(readChunk.array(),0,8178);
			position += 8178;
			byte[] hash = SHA1FromBytes(slice.array());
			chunk.put(hash);
			chunk.put(slice.array());
			slice.clear();
			Arrays.fill(slice.array(),(byte)0);
			for (int i = 0; i < 7; i++) {
				slice.put(readChunk.array(),8178+(i*8194),8194);
				byte[] hash = SHA1FromBytes(slice.array());
				chunk.put(hash);
				chunk.put(slice.array());
				slice.clear();
				Arrays.fill(slice.array(),(byte)0);
			}

			// Let's write this chunk to a file and do a read now...
			data.close();
			// What we want to write is a chunk now
			String chunkFileName = tempFileName + "_chunk" + String.valueOf(0);
			System.out.println(chunkFileName);
			File chunkFile = new File(chunkFileName);
			try {
				if (chunkFile.createNewFile()) {
		        	System.out.println("File created: " + chunkFile.getName());
		      	} else {
		        	System.out.println("File already exists.");
		      	}
		    } catch(IOException e) {
		    	System.out.println("An error occurred.");
	      		e.printStackTrace();
		    }
		    try {
			    //FileWriter myWriter = new FileWriter(chunkFileName);
			    //myWriter.write(chunk.array());
			    //myWriter.close();
			    Files.write(Paths.get(chunkFileName),chunk.array());
			    System.out.println("Successfully wrote to the file.");
			} catch(IOException e) {
				System.out.println("An error occurred.");
	      		e.printStackTrace();
			}

			// Let's read from the chunk we stored
			ByteBuffer storedChunk = ByteBuffer.allocate(BYTES_IN_INT+BYTES_IN_SHORT+BYTES_IN_SHORT+BYTES_IN_LONG+160+65536);
			ByteBuffer cleanStoredChunk = ByteBuffer.allocate(65536);
			data = new RandomAccessFile(chunkFile, "r");
			readValue = data.readFully(storedChunk.array());
			data.close();
			slice.clear();
			Arrays.fill(slice.array(),(byte)0);
			ByteBuffer readHash = ByteBuffer.allocate(20);
			readHash.put(storedChunk.array(),0,20);
			storedChunk.position(20);
			slice.put(storedChunk.array(),storedChunk.position(),8194);
			storedChunk.position(storedChunk.position()+8194);
			byte[] hash = SHA1FromBytes(slice.array());
			if (Arrays.equals(hash, readHash.array())) {
				System.out.println("The first slice isn't corrupt.");
				slice.rewind();
				System.out.println("Sequence: " + slice.getInt());
				System.out.println("Version: " + slice.getShort());
				System.out.println("Chunk size: " + slice.getShort());
				System.out.println("Timestamp: " + slice.getLong());
				cleanStoredChunk.put(slice.array(),16,8178); // Should have enough room
				System.out.println("The hashes match on slice number " + 0 + ". cleanStoredChunk position after write: " + cleanStoredChunk.position());
				//System.out.println(cleanStoredChunk.position());
			} else {
				System.out.println("The hashes don't match on slice number " + 0 + ".");
				System.out.println("Computed hash: " + hash + ", Stored hash: " + readHash);
				break;
			}
			// Clear and reset slice and readHash
			slice.clear();
			Arrays.fill(slice.array(),(byte)0);
			readHash.clear();
			Arrays.fill(readHash.array(),(byte)0);
			for (int i = 0; i < 7; i++) {
				readHash.put(storedChunk.array(),storedChunk.position(),20);
				storedChunk.position(storedChunk.position()+20);
				slice.put(storedChunk.array(),storedChunk.position(),8194);
				storedChunk.position(storedChunk.position()+8194);
				byte[] hash = SHA1FromBytes(slice.array());
				if (Arrays.equals(hash, readHash.array())) {
					cleanStoredChunk.put(slice.array(),0,8194); // Should have enough room
					System.out.println("The hashes match on slice number " + i+1 + ". cleanStoredChunk position after write: " + cleanStoredChunk.position());
					//System.out.println(cleanStoredChunk.position());
				} else {
					System.out.println("The hashes don't match on slice number " + i + ".");
					System.out.println("Computed hash: " + hash + ", Stored hash: " + readHash);
					break;
				}
				// Clear and reset slice and readHash
				slice.clear();
				Arrays.fill(slice.array(),(byte)0);
				readHash.clear();
				Arrays.fill(readHash.array(),(byte)0);
			}

			//readChunk.rewind();
			if (Arrays.equals(cleanStoredChunk.array(),readChunk.array())) {
				System.out.println("Original chunk read matches read on stored chunk.");
			} else {
				System.out.println("The original chunk does not match the stored chunk.");
			}

			// Testing functions
			byte[] testRead = readBytesFromFile(chunkFileName);
			System.out.println("testRead length: " + testRead.length);
			Vector<Integer> errors = checkChunkForCorruption(testRead);
			System.out.println("Number of errors: " + errors.size());
			byte[] testClean = getChunkDataFromArray(testRead);
			System.out.println("testClean length: " + testClean.length);

			if (Arrays.equals(storedChunk.array(),testRead)) {
				System.out.println("testRead matches storedChunk.");
			} else {
				System.out.println("testRead doesn't match storedChunk.");
			}

			if (Arrays.equals(cleanStoredChunk.array(),testClean)) {
				System.out.println("Read function chunk matches original chunk.");
			} else {
				System.out.println("Read function chunk doesn't match stored chunk.");
			}
*/