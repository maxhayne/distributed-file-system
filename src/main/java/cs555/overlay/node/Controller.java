package cs555.overlay.node;
import cs555.overlay.transport.ChunkServerConnectionCache;
import cs555.overlay.util.DistributedFileCache;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.ApplicationProperties;

import java.net.ServerSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;

public class Controller {

	private DistributedFileCache idealState;
	private DistributedFileCache reportedState;
	private ChunkServerConnectionCache connectionCache;

	public Controller() {
		this.idealState = new DistributedFileCache();
		this.reportedState = new DistributedFileCache();
		this.connectionCache = 
			new ChunkServerConnectionCache( idealState, reportedState );
	}

	public static void main(String[] args) throws Exception {
		
		try ( ServerSocket serverSocket = 
			new ServerSocket( ApplicationProperties.controllerPort ) ) {
				
			String host = serverSocket.getInetAddress().getHostAddress();
			Controller controller = new Controller();
			( new Thread ( new TCPServerThread( 
				serverSocket, controller.getConnectionCache() ) ) ).start();
			System.out.println("Controller's ServerThread has started at [" + host 
				+ ":" + Integer.toString( ApplicationProperties.controllerPort ) + "]");
			controller.interact();
		} catch ( IOException ioe ) {
			System.err.println( "Controller failed to start. " + ioe.getMessage());
			System.exit( 1 );
		}
		
	}

	public ChunkServerConnectionCache getConnectionCache() {
		return this.connectionCache;
	}

	public DistributedFileCache getIdealState() {
		return this.idealState;
	}

	public DistributedFileCache getReportedState() {
		return this.reportedState;
	}

	/**
	 * Loop here for user input to the Controller.
	 */
	private void interact() {
		System.out.println( "Enter a command or use 'help' to print a list of commands." );
        Scanner scanner = new Scanner( System.in );
        while ( true ) {
            System.out.print("controller> ");
			String command = scanner.nextLine();
			String[] splitCommand = command.split("\\s+");
            switch ( splitCommand[0].toLowerCase() ) {
                case "help":
                    showHelp();
                    break;

                default:
                    System.err.println( "Unrecognized command. Use 'help' command." );
                    break;
            }
        }
	}

	private void showHelp() {
		System.out.printf( "%3s%-10s : %s%n", "", "help",
                "print a list of valid commands");
	}
}