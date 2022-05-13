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

// This class should maintain an active connection with a chunkserver, which means
// that it should mostly receive heartbeats, and information about storage capacity
public class ChunkServerConnection extends Thread {

	private String serveraddress;
	private int serverport;
	private TCPReceiverThread receiver;
	private int identifier;
	private long freespace;
	private int totalchunks;
	private boolean activestatus;
	private int unhealthy;
	private long starttime;
	private BlockingQueue<byte[]> sendQueue;
	private DataOutputStream dout;	
	private HeartbeatInfo heartbeatinfo; // information about heartbeats
	private int pokes;
	private int pokeReplies;

	public ChunkServerConnection() {
		this.heartbeatinfo = new HeartbeatInfo();
	}

	public ChunkServerConnection(TCPReceiverThread tcpreceiverthread, int identifier, String serveraddress, int serverport) throws IOException {
		this.serveraddress = serveraddress;
		this.serverport = serverport;
		this.receiver = tcpreceiverthread;
		this.identifier = identifier;
		this.freespace = -1;
		this.totalchunks = -1;
		this.starttime = -1;
		this.unhealthy = 0;
		this.activestatus = false;
		this.sendQueue = new LinkedBlockingQueue<byte[]>();
		this.dout = new DataOutputStream(new BufferedOutputStream(tcpreceiverthread.getDataOutputStream(), 8192));
		this.heartbeatinfo = new HeartbeatInfo();
		this.pokes = 0;
		this.pokes = 0;
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

	public synchronized String print() throws UnknownHostException {
		String localAddress = "Unknown";
		int localPort = -1;
		String remoteAddress = "Unknown";
		int remotePort = -1;
		try {
			localAddress = this.getLocalAddress();
			localPort = this.getLocalPort();
			remoteAddress = this.getRemoteAddress();
			remotePort = this.getRemotePort();
		} catch(Exception e) {
			// Nothing to do...
		} 

		String returnable = "";
		String activeString = activestatus ? "active" : "inactive";
		returnable += "[ " + identifier + ", " + remoteAddress + ":" + remotePort + ", " + freespace + ", " + activeString + ", health:" + unhealthy + " ]\n";
		return returnable;
	}

	public void updateHeartbeatInfo(long time, int type, byte[] files) {
		synchronized(heartbeatinfo) {
			heartbeatinfo.update(time,type,files);
		}
	}

	public synchronized void updateFreeSpaceAndChunks(long freespace, int totalchunks) {
		this.freespace = freespace;
		this.totalchunks = totalchunks;
	}

	public synchronized void setStartTime(long time) {
		this.starttime = time;
	}

	public synchronized long getStartTime() {
		return this.starttime;
	}

	public byte[] retrieveHeartbeatInfo() {
		synchronized(heartbeatinfo) {
			return heartbeatinfo.retrieve();
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
		return serveraddress;
	}

	public int getServerPort() {
		return serverport;
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
		freespace = space;
	}

	public synchronized long getFreeSpace() {
		return freespace;
	}

	public synchronized boolean getActiveStatus() {
		return activestatus;
	}

	public synchronized void setActiveStatus(boolean status) {
		activestatus = status;
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