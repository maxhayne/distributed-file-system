package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerNoStoreFile {

	public String address; // Controller will have to do lookup to find the identifier of the server that failed to store the file.
	public String filename;

	public ChunkServerNoStoreFile(String address, String filename) {
		this.address = address;
		this.filename = filename;
	}

	public ChunkServerNoStoreFile(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int addressLength = buffer.getInt();
		byte[] addressarray = new byte[addressLength];
		buffer.get(addressarray);;
		this.address = new String(addressarray);
		int fileLength = buffer.getInt();
		byte[] filearray = new byte[fileLength];
		buffer.get(filearray);
		this.filename = new String(filearray);
	}

	public byte[] getBytes() throws IOException {
 		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeByte(Protocol.CHUNK_SERVER_NO_STORE_FILE);
		byte[] addressarray = address.getBytes();
		dout.writeInt(addressarray.length);
		dout.write(addressarray);
		byte[] filearray = filename.getBytes();
		dout.writeInt(filearray.length);
		dout.write(filearray);
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		return marshalledBytes;
 	}

 	public byte getType() throws IOException {
 		return Protocol.CHUNK_SERVER_NO_STORE_FILE;
 	}
 }