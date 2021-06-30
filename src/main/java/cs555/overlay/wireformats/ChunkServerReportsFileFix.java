package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerReportsFileFix implements Event {

	public int identifier;
	public String filename;

	public ChunkServerReportsFileFix(int identifier, String filename) {
		this.identifier = identifier;
		this.filename = filename;
	}

	public ChunkServerReportsFileFix(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		this.identifier = buffer.getInt();
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.filename = new String(array);
		buffer = null;
		array = null;
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.CHUNK_SERVER_REPORTS_FILE_FIX);
		dout.writeInt(this.identifier);
		byte[] array = this.filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();

		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		array = null;
		return marshalledBytes;
	}

	public byte getType() throws IOException {
		return Protocol.CHUNK_SERVER_REPORTS_FILE_FIX;
	}
}