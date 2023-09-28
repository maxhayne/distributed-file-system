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

	private Socket controllersocket;
	private TCPServerThread server;
	private TCPReceiverThread receiver;
	private BlockingQueue<byte[]> sendQueue;
	private DataOutputStream dout;
	private boolean activestatus;
	private ChunkServerHeartbeatService heartbeatservice;
	private FileDistributionService fileservice;
	private Timer heartbeattimer;
	private String directory;
	private int serveridentifier;

	public ControllerConnection(Socket controllerSocket, ServerSocket serverSocket, String directory) throws IOException {
		this.controllersocket = controllerSocket;
		this.fileservice = new FileDistributionService(directory,this);
		this.server = new TCPServerThread(serverSocket,fileservice);
		this.receiver = new TCPReceiverThread(controllersocket,server,this,this.fileservice);
		this.activestatus = false;
		this.sendQueue = new LinkedBlockingQueue<byte[]>();
		this.dout = new DataOutputStream(new BufferedOutputStream(receiver.getDataOutputStream(), 8192));
		this.directory = directory;
		this.server.start();
		this.receiver.start();
		this.fileservice.start();
		this.serveridentifier = -1;
	}

	public String getServerAddress() throws UnknownHostException {
		return this.receiver.getLocalAddress();
	}

	public int getServerPort() {
		return this.server.getLocalPort();
	}

	public synchronized void setIdentifier(int identifier) {
		this.serveridentifier = identifier;
	}

	public synchronized int getIdentifier() {
		return this.serveridentifier;
	}

	public synchronized void startHeartbeatService() {
		this.heartbeatservice = new ChunkServerHeartbeatService(this.directory,this,this.fileservice);
		this.heartbeattimer = new Timer();
		long randomOffset = (long)ThreadLocalRandom.current().nextInt(2, 15 + 1);
		long heartbeatstart = randomOffset*1000L;
		this.heartbeattimer.scheduleAtFixedRate(heartbeatservice,heartbeatstart,Constants.HEARTRATE);
	}

	public synchronized void stopHeartbeatService() {
		if (heartbeattimer != null) { heartbeattimer.cancel(); }
		heartbeattimer = null;
	}

	public synchronized boolean getActiveStatus() {
		return activestatus;
	}

	public synchronized void setActiveStatus(boolean status) {
		activestatus = status;
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