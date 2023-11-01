package cs555.overlay.transport;

import cs555.overlay.node.Node;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class TCPConnectionCache {

  private final Map<String, TCPConnection> cachedConnections;

  public TCPConnectionCache() {
    this.cachedConnections = new HashMap<>();
  }

  /**
   * Establish a TCPConnection connected to the host:port address specified as a
   * parameter.
   *
   * @param node that connection's events will be processed in
   * @param address host:port string
   * @return TCPConnection to specified host:port
   * @throws IOException if socket can't be created
   * @throws NumberFormatException if provided address isn't formatted
   * correctly, if port in host:port is not a number
   */
  public static TCPConnection establishConnection(Node node, String address)
      throws IOException, NumberFormatException {
    Socket socket = new Socket( address.split( ":" )[0],
        Integer.parseInt( address.split( ":" )[1] ) );
    return new TCPConnection( node, socket );
  }

  // This method should be more robust. It should make an effort to provide
  // the caller with a connection with an active socket. If it is found that
  // the socket is inactive, an attempt should be made to replace the
  // connection with an active one. If that fails, perhaps null should be
  // returned.

  /**
   * Returns a TCPConnection connected to the address specified as a parameter.
   * If a TCPConnection with the specified address already exists in
   * cachedConnections, it is returned, otherwise, a new connection is created,
   * and is added to cachedConnections before being returned.
   *
   * @param node that connection's events will be processed in
   * @param address host:port string
   * @return TCPConnection to specified host:port
   * @throws IOException if connection can't be created
   */
  public synchronized TCPConnection getConnection(Node node, String address,
      boolean start) throws IOException {
    synchronized( cachedConnections ) {
      TCPConnection connection;
      if ( cachedConnections.containsKey( address ) ) {
        connection = cachedConnections.get( address );
      } else {
        connection = establishConnection( node, address );
        cachedConnections.put( address, connection );
      }
      if ( start ) {
        connection.start(); // has no effect if already started
      }
      return connection;
    }
  }

  /**
   * Removes a connection from cachedConnections.
   *
   * @param address of the connection to remove
   */
  public synchronized void removeConnection(String address) {
    cachedConnections.remove( address );
  }

  /**
   * Attempts to close all connections in 'cachedConnections'.
   */
  public synchronized void closeConnections() {
    for ( TCPConnection connection : cachedConnections.values() ) {
      connection.close();
    }
    cachedConnections.clear();
  }
}
