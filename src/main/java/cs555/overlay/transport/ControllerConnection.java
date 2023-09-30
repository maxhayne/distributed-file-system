package cs555.overlay.transport;

import cs555.overlay.node.ChunkServer;
import cs555.overlay.util.ChunkServerHeartbeatService;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.BlockingQueue;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.Socket;
import java.util.Timer;

public class ControllerConnection extends Thread {

	private Socket controllerSocket;
	private TCPServerThread server;
	private TCPReceiverThread receiver;
	private BlockingQueue<byte[]> sendQueue;
	private DataOutputStream dout;
	private boolean activeStatus;
	private ChunkServerHeartbeatService heartbeatService;
	private FileDistributionService fileService;
	private Timer heartbeatTimer;
	private String directory;
	private int serverIdentifier;

	public ControllerConnection(Socket controllerSocket, ServerSocket serverSocket, String directory) throws IOException {
		this.controllerSocket = controllerSocket;
		this.fileService = new FileDistributionService(directory,this);
		this.server = new TCPServerThread(serverSocket,fileService);
		this.receiver = new TCPReceiverThread(controllerSocket,server,this,this.fileService);
		this.activeStatus = false;
		this.sendQueue = new LinkedBlockingQueue<byte[]>();
		this.dout = new DataOutputStream(new BufferedOutputStream(receiver.getDataOutputStream(), 8192));
		this.directory = directory;
		this.server.start();
		this.receiver.start();
		this.fileService.start();
		this.serverIdentifier = -1;
	}

	public String getServerAddress() throws UnknownHostException {
		return this.receiver.getLocalAddress();
	}

	public int getServerPort() {
		return this.server.getLocalPort();
	}

	public synchronized void setIdentifier(int identifier) {
		this.serverIdentifier = identifier;
	}

	public synchronized int getIdentifier() {
		return this.serverIdentifier;
	}

	public synchronized void startHeartbeatService() {
		this.heartbeatService = new ChunkServerHeartbeatService(this.directory, this, this.fileService);
		this.heartbeatTimer = new Timer();
		long randomOffset = (long)ThreadLocalRandom.current().nextInt(2, 15 + 1);
		long heartbeatStart = randomOffset*1000L;
		this.heartbeatTimer.scheduleAtFixedRate(heartbeatService ,heartbeatStart, Constants.HEARTRATE);
	}

	public synchronized void stopHeartbeatService() {
		if (heartbeatTimer != null) { 
			heartbeatTimer.cancel();
		}
		heartbeatTimer = null;
	}

	public synchronized boolean getActiveStatus() {
		return activeStatus;
	}

	public synchronized void setActiveStatus(boolean status) {
		activeStatus = status;
	}

	public boolean addToSendQueue(byte[] msg) {
		try {
			this.sendQueue.put(msg);
		} catch (InterruptedException ie) {
			return false;
		}
		return true;
	}

	public void shutdown() {
		this.stopHeartbeatService();
		this.server.close();
		this.receiver.close();
		this.setActiveStatus(false);
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