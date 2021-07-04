package cs555.overlay.node;
import cs555.overlay.transport.ChunkServerConnectionCache;
import cs555.overlay.util.DistributedFileCache;
import cs555.overlay.transport.TCPServerThread;
import java.net.ServerSocket;
import java.net.InetAddress;

public class Controller {

	public static final int controllerPort = 50000;

	public static void main(String[] args) throws Exception {
		// Create file caches
		DistributedFileCache recommendations = new DistributedFileCache();
		DistributedFileCache state = new DistributedFileCache();
		
		// Create chunk server cache, will also start heartbeat monitor
		ChunkServerConnectionCache chunkcache = new ChunkServerConnectionCache(recommendations,state); // this will start the heartbeats
		InetAddress inetAddress = InetAddress.getLocalHost(); // grabbing local address to pass in registration
		String host = inetAddress.getHostAddress().toString();
		ServerSocket serversocket = new ServerSocket(controllerPort, 32, inetAddress);
		
		// Create server thread, will listen and be able to accept connections with chunk servers and clients
		TCPServerThread server = new TCPServerThread(serversocket,chunkcache);
		server.start();
		System.out.println("Controller's ServerThread has started.");
		// Loop here for user input about statistics for the state of the Distributed File System
	}
}