package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerRequestsFileAcquire implements Event {

	public String filename;
	public String[] servers;

	public ControllerRequestsFileAcquire(String filename, String[] servers) {
		this.filename = filename;
		this.servers = servers;
	}

	public ControllerRequestsFileAcquire(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int filelength = buffer.getInt();
		byte[] array = new byte[filelength];
		buffer.get(array);
		this.filename = new String(array);
		int numServers = buffer.getInt();
		servers = new String[numServers];
		for (int i = 0; i < numServers; i++) {
			int serverlength = buffer.getInt();
			byte[] serverarray = new byte[serverlength];
			buffer.get(serverarray);
			servers[i] = new String(serverarray);
		}
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		dout.writeByte(Protocol.CONTROLLER_REQUESTS_FILE_ACQUIRE);
		byte[] array = filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		if (servers != null) {
			int numServers = servers.length;
			dout.writeInt(numServers);
			for (int i = 0; i < numServers; i++) {
				byte[] serverarray = servers[i].getBytes();
				dout.writeInt(serverarray.length);
				dout.write(serverarray);
			}
		} else {
			dout.writeInt(0);
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
		return Protocol.CONTROLLER_REQUESTS_FILE_ACQUIRE;
	}
}