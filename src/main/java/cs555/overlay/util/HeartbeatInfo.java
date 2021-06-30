package cs555.overlay.util;
import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.Arrays;

public class HeartbeatInfo {

	private long lastMajorHeartbeat;
	private long lastMinorHeartbeat;
	private byte[] files;

	public HeartbeatInfo() {
		this.lastMajorHeartbeat = -1;
		this.lastMinorHeartbeat = -1;
		this.files = null;
	}

	public synchronized void update(long time, int type, byte[] files) {
		if (type == 1)
			this.lastMajorHeartbeat = time;
		else
			this.lastMinorHeartbeat = time;
		this.files = files;
	}

	// So we can return everything at once
	public synchronized byte[] retrieve() {
		int length = 16;
		if (files == null) {
			length += 4;
			byte[] returnable = new byte[length];
			ByteBuffer wrapper = ByteBuffer.wrap(returnable);
			wrapper.putLong(lastMajorHeartbeat);
			wrapper.putLong(lastMinorHeartbeat);
			wrapper.putInt(0);
			return returnable;
		}
		length += files.length;
		byte[] returnable = new byte[length];
		ByteBuffer wrapper = ByteBuffer.wrap(returnable);
		wrapper.putLong(lastMajorHeartbeat);
		wrapper.putLong(lastMinorHeartbeat);
		wrapper.put(files);
		files = null;
		return returnable;
	}
}