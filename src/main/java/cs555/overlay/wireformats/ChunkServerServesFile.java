package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerServesFile implements Event {

	public String filename;
	public byte[] filedata;

	public ChunkServerServesFile(String filename, byte[] filedata) {
		this.filename = filename;
		this.filedata = filedata;
	}

	public ChunkServerServesFile(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int filelength = buffer.getInt();
		byte[] array = new byte[filelength];
		buffer.get(array);
		this.filename = new String(array);
		int datalength = buffer.getInt();
		byte[] dataarray = new byte[datalength];
		buffer.get(dataarray);
		this.filedata = dataarray;
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeByte(Protocol.CHUNK_SERVER_SERVES_FILE);

		byte[] array = filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		dout.writeInt(filedata.length);
		dout.write(filedata);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		return marshalledBytes;
	}

	public byte getType() throws IOException {
		return Protocol.CHUNK_SERVER_SERVES_FILE;
	}
}