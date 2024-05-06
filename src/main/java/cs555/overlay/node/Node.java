package cs555.overlay.node;

import cs555.overlay.transport.TCPConnection;
import cs555.overlay.wireformats.Event;

import java.security.NoSuchAlgorithmException;

/**
 * All node types implement this interface. The goal is to keep all
 * communication between nodes indistinguishable, generalized, such that nodes
 * communicate only with other nodes.
 *
 * @author hayne
 */
public interface Node {
  /**
   * Will be implemented in a specific way for each type of node. Makes it
   * possible for each node to deal with generalized events that occur when a
   * node receives a message in its TCPConnection via its TCPReceiverThread.
   *
   * @param event Event which represents message being handled
   * @param connection TCPConnection which received the event
   */
  void onEvent(Event event, TCPConnection connection);

  /**
   * IP address of the node's server socket.
   *
   * @return host
   */
  String getHost();

  /**
   * Port of the node's server socket.
   *
   * @return port
   */
  int getPort();

}