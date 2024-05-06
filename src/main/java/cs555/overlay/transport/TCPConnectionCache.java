package cs555.overlay.transport;

import cs555.overlay.node.Node;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.Event;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds established TCPConnections for reuse.
 *
 * @author hayne
 */
public class TCPConnectionCache {

  private static final Logger logger = Logger.getInstance();
  private final Node node;
  private final ConcurrentHashMap<String,TCPConnection> cachedConnections;

  /**
   * Only constructor. Creates a new map to hold connections.
   */
  public TCPConnectionCache(Node node) {
    this.node = node;
    this.cachedConnections = new ConcurrentHashMap<>();
  }

  /**
   * Establish a TCPConnection connected to the host:port address specified as a
   * parameter.
   *
   * @param node that connection's events will be processed in
   * @param address host:port string
   * @return TCPConnection to specified host:port, or null if the connection
   * couldn't be established
   */
  public static TCPConnection establishConnection(Node node, String address) {
    try {
      Socket socket = new Socket(address.split(":")[0],
          Integer.parseInt(address.split(":")[1]));
      return new TCPConnection(node, socket);
    } catch (IOException e) {
      logger.debug(e.getMessage());
    }
    return null;
  }

  /**
   * Attempts to send a message to a particular address. Either reuses or
   * creates a new TCPConnection for the task.
   *
   * @param address to send the message to
   * @param event message to be sent
   * @param queue whether to queue the send (true) or not (false)
   * @param start true if TCPReceiverThread should be started, false if not
   * @return true if message was sent, false if not
   */
  public boolean send(String address, Event event, boolean queue,
      boolean start) {

    // What do I want to happen?

    // If a connection exists in cachedConnections, that connection should be
    // used to send the message. If that fails, one more try should be made
    // to establish a connection and send the message. If that succeeds, the
    // connection is added to the map. If it doesn't, nothing is added.

    // If there is no connection in cachedConnections with that address, an
    // attempt is made to establish one, and to send the message. If the
    // message sends, the new connection is added to the map. If it doesn't,
    // nothing is added to the map.

    // The compute function is perfect for this action. It guarantees that we
    // can replace broken connections, if they exist, atomically, by only
    // locking that particular key-value pair, and not the entire map.

    TCPConnection connection =
        cachedConnections.compute(address, (key, value) -> {
          if (value != null && !value.getSocket().isClosed()) {
            if (attemptSend(value, event, queue)) {
              if (start) {
                value.start();
              }
              return value;
            }
          }
          return establishAndSend(address, event, queue, start);
        });
    return connection != null; // true if sent, false if not
  }

  /**
   * Attempts to send a message to a connection. If an IOException is thrown,
   * the connection is closed.
   *
   * @param connection to send message to
   * @param event message to send
   * @param queue whether to queue the send (true) or not (false)
   * @return true if message was sent, false otherwise
   */
  private boolean attemptSend(TCPConnection connection, Event event,
      boolean queue) {
    try {
      if (queue) {
        connection.getSender().queueSend(event.getBytes());
      } else {
        connection.getSender().sendData(event.getBytes());
      }
    } catch (IOException e) {
      // TODO IOException could have been thrown by event.getBytes(), and not
      //  because the socket is broken...
      logger.debug("Event " + event.getType() + " not sent. " + e.getMessage());
      connection.close();
      return false;
    }
    return true;
  }

  /**
   * Establishes a new connection and sends a message. If the connection
   * couldn't be established or the message couldn't be sent, null is returned.
   * Otherwise, the newly established connection is returned.
   *
   * @param address to connect to
   * @param event message to send
   * @param queue whether to queue the send (true) or not (false)
   * @param start true if TCPReceiverThread should be started, false if not
   * @return TCPConnection of new connection, null if connection couldn't be
   * established or message couldn't be sent
   */
  private TCPConnection establishAndSend(String address, Event event,
      boolean queue, boolean start) {
    TCPConnection connection = establishConnection(node, address);
    if (connection != null && attemptSend(connection, event, queue)) {
      if (start) {
        connection.start();
      }
      return connection;
    }
    return null;
  }

  /**
   * Removes a connection from cachedConnections. If a connection was removed,
   * closes it too.
   *
   * @param address of connection to remove
   */
  private void removeConnection(String address) {
    TCPConnection connection = cachedConnections.remove(address);
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Attempts to close all connections in cachedConnections, and clears it of
   * entries.
   */
  public void closeConnections() {
    cachedConnections.forEach((address, connection) -> {
      connection.close();
    });
    cachedConnections.clear();
  }
}