package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ControllerDeniesStorageRequest implements Event {

	public ControllerDeniesStorageRequest() {
		// Empty constructor
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeByte(Protocol.CONTROLLER_DENIES_STORAGE_REQUEST);
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		baOutputStream = null;
		dout = null;
		return marshalledBytes;
	}

	public byte getType() throws IOException {
		return Protocol.CONTROLLER_DENIES_STORAGE_REQUEST;
	}
}