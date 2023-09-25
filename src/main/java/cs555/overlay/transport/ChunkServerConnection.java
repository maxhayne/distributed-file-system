package cs555.overlay.transport;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import cs555.overlay.util.HeartbeatInfo;
import java.util.concurrent.TimeUnit;
import java.net.UnknownHostException;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.io.IOException;
import java.util.Vector;
import java.net.Socket;

/**
 * Maintains an active connection with a ChunkServer. Its function is to
 * maintain status information and to actively send messages in its
 * queue to the ChunkServer it's connected with.
 */
public class ChunkServerConnection extends Thread {

	// Identifying/Connection information
	private int identifier;
	private String serverAddress;
	private int serverPort;
	private TCPReceiverThread receiver;
	private BlockingQueue<byte[]> sendQueue;
	private DataOutputStream dout;	
	
	// Status information
	private long freeSpace;
	private int totalChunks;
	private boolean activeStatus;
	private int unhealthy;
	private long startTime;
	private HeartbeatInfo heartbeatInfo; // information about heartbeats
	private int pokes;
	private int pokeReplies;

	public ChunkServerConnection() {
		this.heartbeatInfo = new HeartbeatInfo();
	}

	public ChunkServerConnection(TCPReceiverThread receiver, int identifier, 
			String serverAddress, int serverPort) throws IOException {
		this.identifier = identifier;
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
		this.receiver = receiver;
		this.sendQueue = new LinkedBlockingQueue<byte[]>();
		this.dout = new DataOutputStream(
			new BufferedOutputStream(receiver.getDataOutputStream(), 8192));
		this.freeSpace = -1;
		this.totalChunks = -1;
		this.startTime = -1;
		this.unhealthy = 0;
		this.activeStatus = false;
		this.heartbeatInfo = new HeartbeatInfo();
		this.pokes = 0;
		this.pokeReplies = 0;
	}

	public synchronized void incrementPokes() {
		this.pokes++;
	}

	public synchronized void incrementPokeReplies() {
		this.pokeReplies++;
	}

	public synchronized int getPokeDiscrepancy() {
		return (this.pokes-this.pokeReplies);
	}

	public synchronized String print() {
		String remoteAddress = "Unknown";
		int remotePort = -1;
		try {
			remoteAddress = this.getRemoteAddress();
			remotePort = this.getRemotePort();
		} catch(Exception e) {} 

		String returnable = "";
		String activeString = activeStatus ? "active" : "inactive";
		returnable += "[ " + identifier + ", " + remoteAddress + ":" + remotePort 
			+ ", " + freeSpace + ", " + activeString + ", health:" + unhealthy + " ]\n";
		return returnable;
	}

	public void updateHeartbeatInfo(long time, int type, byte[] files) {
		synchronized(heartbeatInfo) {
			heartbeatInfo.update(time,type,files);
		}
	}

	public synchronized void updateFreeSpaceAndChunks(long freeSpace, int totalChunks) {
		this.freeSpace = freeSpace;
		this.totalChunks = totalChunks;
	}

	public synchronized void setStartTime(long time) {
		this.startTime = time;
	}

	public synchronized long getStartTime() {
		return this.startTime;
	}

	public byte[] retrieveHeartbeatInfo() {
		synchronized(heartbeatInfo) {
			return heartbeatInfo.retrieve();
		}
	}

	public int getIdentifier() {
		return identifier;
	}

	public String getLocalAddress() throws UnknownHostException {
		return receiver.getLocalAddress();
	}

	public String getRemoteAddress() {
		return receiver.getRemoteAddress();
	}

	public int getLocalPort() {
		return receiver.getLocalPort();
	}

	public int getRemotePort() {
		return receiver.getRemotePort();
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public int getServerPort() {
		return serverPort;
	}

	public synchronized int getUnhealthy() {
		return unhealthy;
	}

	public synchronized void setUnhealthy(int unhealthy) {
		this.unhealthy = unhealthy;
	}

	public synchronized void incrementUnhealthy() {
		this.unhealthy += 1;
	}

	public synchronized void decrementUnhealthy() {
		if (unhealthy > 0)
			unhealthy -= 1;
	}

	public synchronized void setFreeSpace(long space) {
		freeSpace = space;
	}

	public synchronized long getFreeSpace() {
		return freeSpace;
	}

	public synchronized boolean getActiveStatus() {
		return activeStatus;
	}

	public synchronized void setActiveStatus(boolean status) {
		activeStatus = status;
	}

	public void close() {
		receiver.close();
	}

	// use to send to this chunk server
	public boolean addToSendQueue(byte[] msg) {
		try {
			this.sendQueue.put(msg);
		} catch (InterruptedException ie) {
			return false;
		}
		return true;
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
				System.err.println("ChunkServerConnection run InterruptedException: " + ie);
			}
			if (msg == null) {
				continue;
			} else {
				try {
					this.sendData(msg); // send the message along
				} catch (Exception e) {
					System.err.println("ChunkServerConnection run Exception: " + e);
				}
			}
		}
	}
}