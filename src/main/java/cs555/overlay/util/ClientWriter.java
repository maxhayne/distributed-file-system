package cs555.overlay.util;

import cs555.overlay.transport.TCPSender;

/**
 * Helper class used by the Client to read a file from disk and send (write) its
 * chunks to the DFS.
 *
 * @author hayne
 */
public class ClientWriter implements Runnable {

  private final TCPSender controllerConnection;

  public ClientWriter(TCPSender controllerConnection) {
    this.controllerConnection = controllerConnection;
  }

  public TCPSender getSender() {
    return this.controllerConnection;
  }

  @Override
  public void run() {
    System.out.println( "Writer run called." );
  }
}