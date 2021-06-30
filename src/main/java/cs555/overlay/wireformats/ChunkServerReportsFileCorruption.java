package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerReportsFileCorruption implements Event {

	public int identifier;
	public String filename;
	public int[] slices;

	public ChunkServerReportsFileCorruption(int identifier, String filename, int[] slices) {
		this.identifier = identifier;
		this.filename = filename;
		this.slices = slices;
	}

	public ChunkServerReportsFileCorruption(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		this.identifier = buffer.getInt();
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.filename = new String(array);
		int slicelength = (int)buffer.get();
		if (slicelength > 0) {
			slices = new int[slicelength];
			for (int i = 0; i < slicelength; i++) {
				slices[i] = (int)buffer.get();
			}
		} else {
			slices = null;
		}
		buffer = null;
		array = null;
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION);
		dout.writeInt(this.identifier);
		byte[] array = this.filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		if (slices == null) {
			dout.writeByte((byte)0);
		} else {
			dout.writeByte((byte)slices.length);
			for (int i = 0; i < slices.length; i++)
				dout.writeByte((byte)slices[i]);
		}

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
		return Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION;
	}
}