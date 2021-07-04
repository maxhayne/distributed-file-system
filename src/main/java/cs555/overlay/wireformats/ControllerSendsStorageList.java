package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerSendsStorageList implements Event {

	public String filename;
	public String[] replicationservers = null;
	public String[] shardservers = null;

	public ControllerSendsStorageList(String filename, String[] replicationservers, String[] shardservers) {
		this.filename = filename;
		if (replicationservers.length == 1 && replicationservers[0].equals(""))
			replicationservers = null;
		else 
			this.replicationservers = replicationservers;
		if (shardservers.length == 1 && shardservers[0].equals(""))
			this.shardservers = null;
		else
			this.shardservers = shardservers;
	}

	public ControllerSendsStorageList(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int length = buffer.getInt();
		byte[] string = new byte[length];
		buffer.get(string);
		this.filename = new String(string);
		int replicationcount = buffer.getInt();
		if (replicationcount != 0) {
			this.replicationservers = new String[replicationcount];
			for (int i = 0; i < replicationcount; i++) {
				length = buffer.getInt();
				string = new byte[length];
				buffer.get(string);
				replicationservers[i] = new String(string);
			}
		}
		int shardcount = buffer.getInt();
		if (shardcount != 0) {
			this.shardservers = new String[shardcount];
			for (int i = 0; i < shardcount; i++) {
				length = buffer.getInt();
				string = new byte[length];
				buffer.get(string);
				shardservers[i] = new String(string);
			}
		}
		string = null;
		buffer = null;
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		// Write type and filename
		dout.writeByte(Protocol.CONTROLLER_SENDS_STORAGE_LIST);
		byte[] array = this.filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);

		// Write list of replication server address:port
		int replicationserverlength = 0;
		if (replicationservers == null)
			dout.writeInt(0);
		else {
			replicationserverlength = replicationservers.length;
			dout.writeInt(replicationservers.length);
		}

		for (int i = 0; i < replicationserverlength; i++) {
			array = replicationservers[i].getBytes();
			dout.writeInt(array.length);
			dout.write(array);
		}

		// Write list of shard server address:port
		int shardserverlength = 0;
		if (shardservers == null)
			dout.writeInt(0);
		else {
			shardserverlength = shardservers.length;
			dout.writeInt(shardservers.length);
		}

		for (int i = 0; i < shardserverlength; i++) {
			array = shardservers[i].getBytes();
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
		return Protocol.CONTROLLER_SENDS_STORAGE_LIST;
	}
}
