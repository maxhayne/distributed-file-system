package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerSendsRegistration implements Event {

	public String serverAddress;
	public int serverPort;

	public ChunkServerSendsRegistration(String serverAddress, int serverPort) {
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
	}

	public ChunkServerSendsRegistration(byte[] msg) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.serverAddress = new String(array);
		this.serverPort = buffer.getInt();
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.CHUNK_SERVER_SENDS_REGISTRATION);
		byte[] array = this.serverAddress.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		dout.writeInt(this.serverPort);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();

		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public byte getType() {
		return Protocol.CHUNK_SERVER_SENDS_REGISTRATION;
	}
}