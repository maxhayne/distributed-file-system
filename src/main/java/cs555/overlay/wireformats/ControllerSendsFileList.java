package cs555.overlay.wireformats;

import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerSendsFileList implements Event {

	public String[] list;

	public ControllerSendsFileList(String[] list) {
		this.list = list;
	}

	public ControllerSendsFileList(byte[] msg) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		int listLength = buffer.getInt();
		if (listLength == 0) {
			this.list = null;
		} else {
			this.list = new String[listLength];
			for (int i = 0; i < listLength; i++) {
				int arrayLength = buffer.getInt();
				byte[] fileArray = new byte[arrayLength];
				buffer.get(fileArray);
				this.list[i] = new String(fileArray);
			}
		}
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeByte(Protocol.CONTROLLER_SENDS_FILE_LIST);
		if (list == null) {
			dout.writeInt(0);
		} else {
			dout.writeInt(list.length);
			for (int i = 0; i < list.length; i++) {
				byte[] fileArray = list[i].getBytes();
				dout.writeInt(fileArray.length);
				dout.write(fileArray);
			}
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
		return Protocol.CONTROLLER_SENDS_FILE_LIST;
	}
}