package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerReportsChunkServerRegistrationStatus implements Event {

	public int status;
	public String info;

	public ControllerReportsChunkServerRegistrationStatus(int status, String info) {
		this.status = status;
		this.info = info;
	}

	public ControllerReportsChunkServerRegistrationStatus(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		this.status = buffer.getInt();
		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.info = new String(array);
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeByte(Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS);
		dout.writeInt(status);
		byte[] array = this.info.getBytes();
		dout.writeInt(array.length);
		dout.write(array);

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
		return Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS;
	}
}