package cs555.overlay.transport;

import cs555.overlay.node.Node;

import java.io.IOException;
import java.net.Socket;

/**
 * Class to hold information about a socket connection between the current node
 * another node on the network. Has functionality to send and receive messages
 * over its socket. Contains an active TCPReceiverThread which automatically
 * receives and parses messages.
 *
 * @author hayne
 */
public class TCPConnection {

  private final Socket socket;
  private final TCPSender sender;
  private final TCPReceiverThread receiver;

  /**
   * Default constructor.
   *
   * @param node node TCPConnection is a part of
   * @param socket socket of the connection
   * @throws IOException if data input/output streams fail to open
   */
  public TCPConnection(Node node, Socket socket) throws IOException {
    this.socket = socket;
    this.sender = new TCPSender( socket );
    this.receiver = new TCPReceiverThread( node, socket, this );
  }

  /**
   * The TCPReceiverThread object has been created, but a thread to encapsulate
   * it hasn't been. This creates and starts that thread to start receiving
   * messages concurrently.
   */
  public void start() {
    (new Thread( receiver )).start();
  }

  /**
   * Getter for socket.
   *
   * @return connection socket
   */
  public Socket getSocket() {
    return socket;
  }

  /**
   * Getter for TCPSender.
   *
   * @return connection's TCPSender
   */
  public TCPSender getSender() {
    return sender;
  }

  /**
   * Close this connection's socket. If the receiver thread has been started,
   * this will stop the thread as well.
   */
  public void close() {
    try {
      sender.dout.close();
      receiver.din.close();
      socket.close();
    } catch ( IOException ioe ) {
      System.err.println( "Problem closing socket/streams. "+ioe.getMessage() );
    }
  }
}
