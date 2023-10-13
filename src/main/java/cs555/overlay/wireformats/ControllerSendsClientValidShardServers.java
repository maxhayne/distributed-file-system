package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerSendsClientValidShardServers implements Event {

	public String filename;
	public int sequence;
	public String[] servers; // Will be in format 'IP:PORT'

	public ControllerSendsClientValidShardServers( String filename, int sequence, String[] servers ) {
		this.filename = filename;
		this.sequence = sequence;
		this.servers = servers;
	}

	public ControllerSendsClientValidShardServers(byte[] msg) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.filename = new String(array);
		this.sequence = buffer.getInt();
		int numServers = buffer.getInt();
		this.servers = new String[numServers];
		for (int i = 0; i < numServers; i++) {
			int serverlength = buffer.getInt();
			byte[] server = new byte[serverlength];
			buffer.get(server);
			this.servers[i] = new String(server);
		}
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		dout.writeByte(Protocol.CONTROLLER_SENDS_CLIENT_VALID_SHARD_SERVERS);
		byte[] array = filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		dout.writeInt(sequence);
		int numServers = servers.length;
		dout.writeInt(numServers);
		for (int i = 0; i < numServers; i++) {
			byte[] server = servers[i].getBytes();
			dout.writeInt(server.length);
			dout.write(server); 
		}
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		return marshalledBytes;
	}

	public byte getType() {
		return Protocol.CONTROLLER_SENDS_CLIENT_VALID_SHARD_SERVERS;
	} 
}