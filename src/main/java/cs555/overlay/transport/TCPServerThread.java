package cs555.overlay.transport;
import cs555.overlay.util.FileDistributionService;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.Socket;

public class TCPServerThread extends Thread {
	
	private ServerSocket server;
	private boolean controller;

	// Will only be used at the controller
	private ChunkServerConnectionCache chunkcache = null;
	// Will only be used at a chunk server
	private FileDistributionService fileservice = null;

	// Constructor for controller
	public TCPServerThread(ServerSocket socket, ChunkServerConnectionCache chunkcache) {
		this.server = socket;
		this.chunkcache = chunkcache;
		this.controller = true;
	}

	// Constructor for chunk server
	public TCPServerThread(ServerSocket socket, FileDistributionService fileservice) {
		this.server = socket;
		this.fileservice = fileservice;
		this.controller = false;
	}

	public int getLocalPort() {
		return server.getLocalPort();
	}

	public void close() {
		try {
			if (server != null)
				server.close();
		} catch (IOException ioe) {
			System.out.println("TCPServerThread close IOException: " + ioe);
		}
		server = null;
	}

	@Override
	public void run() {
		try {
			while(true) {
				Socket incomingConnectionSocket = server.accept(); // accept new connections
				// Ternary operator for creating a new receiver, is the receiver at the controller or not?
				Thread receiverThread = this.controller 
					? new Thread(new TCPReceiverThread(incomingConnectionSocket,this,this.chunkcache)) 
					: new Thread(new TCPReceiverThread(incomingConnectionSocket,this,this.fileservice));
				receiverThread.start();
				System.out.println("Accepted a new connection!");
			}
		} catch (IOException ioe) {
			System.err.println("TCPServerThread run IOException: " + ioe);
		} finally {
			try {
				if (server != null)
					server.close();
			} catch (IOException ioe) {
				System.err.println("TCPServerThread run IOException: " + ioe);
			}
		}
		this.close();
		System.out.println("TCPServerThread has stopped.");
	}
}