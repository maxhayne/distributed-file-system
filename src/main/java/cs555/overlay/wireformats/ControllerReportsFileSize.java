package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerReportsFileSize implements Event {

	public String filename;
	public int totalchunks;

	public ControllerReportsFileSize(String filename, int totalchunks) {
		this.filename = filename;
		this.totalchunks = totalchunks;
	}

	public ControllerReportsFileSize(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int fileLength = buffer.getInt();
		byte[] filearray = new byte[fileLength];
		buffer.get(filearray);
		this.filename = new String(filearray);
		this.totalchunks = buffer.getInt();
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.CONTROLLER_REPORTS_FILE_SIZE);
		byte[] array = filename.getBytes();
		dout.writeInt(array.length);
		dout.write(array);
		dout.writeInt(totalchunks);

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
		return Protocol.CONTROLLER_REPORTS_FILE_SIZE;
	}
}