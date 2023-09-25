package cs555.overlay.transport;
import cs555.overlay.util.FileDistributionService;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.Socket;

public class TCPServerThread extends Thread {
	
	private ServerSocket server;
	private boolean controller;

	private ChunkServerConnectionCache connectionCache; // used at Controller
	private FileDistributionService fileService; // used at ChunkServer

	// Constructor for Controller
	public TCPServerThread(ServerSocket socket, ChunkServerConnectionCache connectionCache) {
		this.server = socket;
		this.connectionCache = connectionCache;
		this.controller = true;
	}

	// Constructor for ChunkServer
	public TCPServerThread(ServerSocket socket, FileDistributionService fileService) {
		this.server = socket;
		this.fileService = fileService;
		this.controller = false;
	}

	public int getLocalPort() {
		return server.getLocalPort();
	}

	public void close() {
		try {
			if (server != null) server.close();
		} catch (IOException ioe) {}
	}

	@Override
	public void run() {
		try {
			while(true) {
				Socket incomingConnectionSocket = server.accept(); // accept new connections
				// Ternary operator for creating a new receiver, is the receiver at the controller or not?
				Thread receiverThread = this.controller 
					? new Thread(new TCPReceiverThread(incomingConnectionSocket,this,this.connectionCache)) 
					: new Thread(new TCPReceiverThread(incomingConnectionSocket,this,this.fileService));
				receiverThread.start();
			}
		} catch (IOException ioe) {
			//System.err.println("TCPServerThread run IOException: " + ioe);
		} finally {
			try {
				if (server != null)
					server.close();
			} catch (IOException ioe) {
				//System.err.println("TCPServerThread run IOException: " + ioe);
			}
		}
		this.close();
		System.out.println("Server has stopped.");
	}
}