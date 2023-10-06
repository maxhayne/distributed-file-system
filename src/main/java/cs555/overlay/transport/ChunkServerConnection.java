package cs555.overlay.transport;

import cs555.overlay.util.HeartbeatInfo;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

	private String host;
	private int port;
	private TCPConnection connection;

	// Identifying/Connection information
	private int identifier;
	private String serverAddress;
	private int serverPort;
	private TCPReceiverThread receiver;
	private BlockingQueue<byte[]> sendQueue;
	private DataOutputStream dout;	
	
	// Status information
	private long startTime;
	private int unhealthy;
	private boolean activeStatus;
	private HeartbeatInfo heartbeatInfo; // latest heartbeat info
	private int pokes;
	private int pokeReplies;

	public ChunkServerConnection() {
		this.heartbeatInfo = new HeartbeatInfo();
	}

	public ChunkServerConnection( TCPReceiverThread receiver, int identifier, 
			String serverAddress, int serverPort ) throws IOException {
		this.identifier = identifier;
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
		this.receiver = receiver;
		this.sendQueue = new LinkedBlockingQueue<byte[]>();
		this.dout = new DataOutputStream(
			new BufferedOutputStream(receiver.getDataOutputStream(), 8192));
		
		this.startTime = -1;
		this.unhealthy = 0;
		this.activeStatus = false;
		this.heartbeatInfo = new HeartbeatInfo();
		this.pokes = 0;
		this.pokeReplies = 0;
	}

	public ChunkServerConnection( int identifier, String host, int port, 
			TCPConnection connection ) {
		this.identifier = identifier;
		this.host = host;
		this.port = port;
		this.connection = connection;
		this.sendQueue = new LinkedBlockingQueue<byte[]>(); // useful for TCPSender?

		this.startTime = System.currentTimeMillis();
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

	public HeartbeatInfo getHeartbeatInfo() {
		return heartbeatInfo;
	}

	public synchronized long getStartTime() {
		return this.startTime;
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

	public synchronized boolean getActiveStatus() {
		return activeStatus;
	}

	public synchronized void setActiveStatus(boolean status) {
		activeStatus = status;
	}

	public void close() {
		receiver.close();
	}

	public synchronized String print() {
		String remoteAddress = "Unknown";
		int remotePort = -1;
		try {
			remoteAddress = this.getRemoteAddress();
			remotePort = this.getRemotePort();
		} catch( Exception e ) {
			// If we can't get the remote address and port,
			// we'll just print "Unknown" and "-1".
		} 

		String activeString = activeStatus ? "active" : "inactive";
		String returnable = "[ " + identifier + ", " + remoteAddress + ":" + remotePort 
			+ ", " + heartbeatInfo.getFreeSpace() + ", " + activeString + ", health:"
			+ unhealthy + " ]\n";
		return returnable;
	}

	// use to send to this chunk server
	public boolean addToSendQueue( byte[] msg ) {
		try {
			this.sendQueue.put( msg );
		} catch (InterruptedException ie) {
			return false;
		}
		return true;
	}

	public synchronized void sendData(byte[] data) throws IOException {
		int length = data.length;
		dout.writeInt( length );
		dout.write( data, 0, length );
		dout.flush();
	}

	@Override
	public void run() {
		this.setActiveStatus(true);
		while( this.getActiveStatus() ) {
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
					this.sendData( msg ); // send the message along
				} catch (Exception e) {
					System.err.println("ChunkServerConnection run Exception: " + e);
				}
			}
		}
	}
}