package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerSendsHeartbeat implements Event {

	public int type; // 1 is major, 0 is minor
	public int totalchunks;
	public long freespace;
	public String[] files = null;

	public ChunkServerSendsHeartbeat(int type, int totalchunks, long freespace, String[] files) {
		this.type = type;
		this.totalchunks = totalchunks;
		this.freespace = freespace;
		this.files = files;
	}

	public ChunkServerSendsHeartbeat(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		this.type = buffer.getInt();
		this.totalchunks = buffer.getInt();
		this.freespace = buffer.getLong();
		int fileslength = buffer.getInt();
		if (fileslength != 0) {
			files = new String[fileslength];
			for (int i = 0; i < fileslength; i++) {
				int length = buffer.getInt();
				byte[] array = new byte[length];
				buffer.get(array);
				files[i] = new String(array);
			}
		} else {
			files = null;
		}
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		dout.writeByte(Protocol.CHUNK_SERVER_SENDS_HEARTBEAT);
		dout.writeInt(type);
		dout.writeInt(totalchunks);
		dout.writeLong(freespace);
		int fileslength = files == null ? 0 : files.length;
		dout.writeInt(fileslength);
		for (int i = 0; i < fileslength; i++) {
			byte[] array = files[i].getBytes();
			dout.writeInt(array.length);
			dout.write(array);
		}

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		return marshalledBytes;
	}

	public byte getType() throws IOException {
		return Protocol.CHUNK_SERVER_SENDS_HEARTBEAT;
	}
}