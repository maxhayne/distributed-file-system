package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SendsFileForStorage implements Event {

	public String filename;
	public byte[] filedata;
	public String[] servers;

	public SendsFileForStorage(String filename, byte[] filedata, String[] servers) {
		this.filename = filename;
		this.filedata = filedata;
		this.servers = servers;
	}

	public SendsFileForStorage(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.filename = new String(array);
		int datalength = buffer.getInt();
		byte[] data = new byte[datalength];
		buffer.get(data);
		this.filedata = data;
		int numServers = buffer.getInt();
		if (numServers != 0) {
			this.servers = new String[numServers];
			for (int i = 0; i < numServers; i++) {
				int serverlength = buffer.getInt();
				byte[] serverarray = new byte[serverlength];
				buffer.get(serverarray);
				this.servers[i] = new String(serverarray);
			}
		} else {
			this.servers = null;
		}
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeByte(Protocol.SENDS_FILE_FOR_STORAGE);
		byte[] array = filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		dout.writeInt(filedata.length);
		dout.write(filedata);
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
		return Protocol.SENDS_FILE_FOR_STORAGE;
	}
}