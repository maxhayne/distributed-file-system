package cs555.overlay.node;
import cs555.overlay.wireformats.ChunkServerSendsRegistration;
import cs555.overlay.transport.ControllerConnection;
import cs555.overlay.util.ApplicationProperties;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.io.File;

public class ChunkServer {
	
	public static void main(String[] args) throws Exception {
		// CHECK ARGUMENTS. NEED PATH, OPTIONAL PORT
		if (args.length > 2 || args.length < 1)
			System.out.println("Only two command line arguments are accepted: the "
				+ "directory into which it will store files, and the (optional) port "
				+ "number on which the ChunkServer will operate.");
		String directoryString = args[0];
		File directory = new File(directoryString);
		if (!directory.isDirectory()) {
			System.err.println("Error: '" + directoryString + "' is not a valid "
				+ "directory. It either doesn't exist, or it isn't a directory.");
			return;
		}
		int accessPort = 0;
		if (args.length == 2) {
			try {
				accessPort = Integer.parseInt(args[1]);
				if (accessPort > 65535 || accessPort < 1024) {
					System.out.println("The port number must be between 1024 and "
						+ "65535. An open port will be chosen automatically.");
					accessPort = 0;
				}
			} catch (NumberFormatException e) {
				System.err.println("Error: '" + args[0] + "' is not an integer. An "
					+ "open port will be chosen automatically.");
				accessPort = 0;
			}
		}
		if (!directoryString.endsWith("/"))
			directoryString += "/";
		System.out.println(directoryString);
		// END OF CHECKING ARGUMENTS

		// Let's try to connect to the Controller as a chunk server
		try {
			InetAddress inetAddress = InetAddress.getLocalHost(); // grabbing local address to pass in registration
			//InetAddress inetAddress = InetAddress.getByName( "localhost" ); // making the local address the loopback
			String host = inetAddress.getHostAddress().toString();
			ServerSocket serverSocket = new ServerSocket(0,32,inetAddress);
			Socket controllerSocket = new Socket(ApplicationProperties.controllerHost, ApplicationProperties.controllerPort);
			System.out.println("[" + host + ":" + serverSocket.getLocalPort() + "]");
			ControllerConnection controllerConnection = new ControllerConnection(controllerSocket,serverSocket,directoryString);
			controllerConnection.start(); // Starts the sendQueue
			ChunkServerSendsRegistration registration = new ChunkServerSendsRegistration(host,serverSocket.getLocalPort());
			controllerConnection.addToSendQueue(registration.getBytes());
		} catch (Exception e) {
			System.err.println("There was a problem setting up a socket connection to the controller.");
			System.err.println(e);
			return;
		}
	}
}