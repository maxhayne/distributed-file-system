package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RequestsSlices implements Event {

	public String filename;
	public int[] slices;

	public RequestsSlices(String filename, int[] slices) {
		this.filename = filename;
		this.slices = slices;
	}

	public RequestsSlices(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int fileLength = buffer.getInt();
		byte[] filearray = new byte[fileLength];
		buffer.get(filearray);
		this.filename = new String(filearray);
		int numSlices = buffer.getInt();
		this.slices = new int[numSlices];
		for (int i = 0; i < numSlices; i++) {
			this.slices[i] = buffer.getInt();
		}
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.REQUESTS_SLICES);
		byte[] array = filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		int numSlices = slices.length;
		dout.writeInt(numSlices);
		for (int i = 0; i < numSlices; i++) {
			dout.writeInt(slices[i]);
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
		return Protocol.REQUESTS_SLICES;
	}
}