package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ControllerApprovesFileDelete implements Event {

	public ControllerApprovesFileDelete() {
		// Empty constructor
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeByte(Protocol.CONTROLLER_APPROVES_FILE_DELETE);
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		return marshalledBytes;
	}

	public byte getType() {
		return Protocol.CONTROLLER_APPROVES_FILE_DELETE;
	}
}