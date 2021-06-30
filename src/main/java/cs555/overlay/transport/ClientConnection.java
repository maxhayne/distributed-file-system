package cs555.overlay.transport;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;


public class ClientConnection extends Thread {

	private TCPReceiverThread receiver;
	private BlockingQueue<byte[]> sendQueue;
	private DataOutputStream dout;
	private boolean activeStatus;
	private int identifier;

	public ClientConnection(TCPReceiverThread tcpreceiverthread, int identifier) throws IOException {
		this.receiver = tcpreceiverthread;
		this.identifier = identifier;
		this.activeStatus = false;
		this.sendQueue = new LinkedBlockingQueue<byte[]>();
		this.dout = new DataOutputStream(new BufferedOutputStream(tcpreceiverthread.getDataOutputStream(), 8192));
	}

	public int getIdentifier() {
		return identifier;
	}

	public synchronized boolean getActiveStatus() {
		return activeStatus;
	}

	public synchronized void setActiveStatus(boolean status) {
		activeStatus = status;
	}

	public synchronized void sendData(byte[] data) throws IOException {
		int length = data.length;
		dout.writeInt(length);
		dout.write(data, 0, length);
		dout.flush();
	}

	@Override
	public void run() {
		this.setActiveStatus(true);
		while(this.getActiveStatus()) {
			byte[] msg = null;
			try {
				msg = this.sendQueue.poll(1, TimeUnit.SECONDS);
			} catch (InterruptedException ie) {
				System.err.println("ClientConnection run InterruptedException: " + ie);
			}
			if (msg == null) {
				continue;
			} else {
				try {
					this.sendData(msg); // send the message along
				} catch (Exception e) {
					System.err.println("ClientConnection run Exception: " + e);
				}
			}
		}
	}
}