package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerSendsRegistration implements Event {

	public String serveraddress;
	public int serverport;

	public ChunkServerSendsRegistration(String serveraddress, int serverport) {
		this.serveraddress = serveraddress;
		this.serverport = serverport;
	}

	public ChunkServerSendsRegistration(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.serveraddress = new String(array);
		this.serverport = buffer.getInt();
		buffer = null;
		array = null;
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.CHUNK_SERVER_SENDS_REGISTRATION);
		byte[] array = this.serveraddress.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		dout.writeInt(this.serverport);

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
		return Protocol.CHUNK_SERVER_SENDS_REGISTRATION;
	}
}