package cs555.overlay.transport;

import cs555.overlay.node.Node;
import cs555.overlay.util.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Class to encapsulate a server socket, which will be available to connect to
 * at every node instance in the system.
 *
 * @author hayne
 */
public class TCPServerThread implements Runnable {

  private static final Logger logger = Logger.getInstance();
  private final Node node;
  private final ServerSocket serverSocket;

  /**
   * Default constructor.
   *
   * @param node node the server is being run on
   * @param serverSocket ServerSocket the thread will be looping
   */
  public TCPServerThread(Node node, ServerSocket serverSocket) {
    this.node = node;
    this.serverSocket = serverSocket;
  }

  /**
   * Loops over ServerSocket while it is non-null. Accepts connections when
   * available, and creates new TCPConnections with active TCPReceiverThreads
   * for those new connections.
   */
  @Override
  public void run() {
    while ( serverSocket != null ) {
      try {
        Socket newSocket = serverSocket.accept();
        (new TCPConnection( node, newSocket )).start();
      } catch ( IOException ioe ) {
        logger.debug( "ServerSocket has stopped. "+ioe.getMessage() );
        break;
      }

    }
  }
}
