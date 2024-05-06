package cs555.overlay.transport;

import cs555.overlay.util.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple class to send out of the DataOutputStream associated with the Socket
 * passed in the constructor.
 *
 * @author hayne
 */
public class TCPSenderThread implements Runnable {

  private static final Logger logger = Logger.getInstance();
  protected DataOutputStream dout;
  private final LinkedBlockingQueue<byte[]> sendQueue;
  private volatile boolean active;

  /**
   * Creates DataOutputStream based on the passed Socket, and a send queue for
   * holding messages.
   *
   * @param socket to open the DataOutputStream on
   * @throws IOException if stream can't be created
   */
  public TCPSenderThread(Socket socket) throws IOException {
    this.dout = new DataOutputStream(socket.getOutputStream());
    // TODO Think about a size limit for the sendQueue -- should it mirror
    //  the size of the socket receive and send buffers? Should it be limited
    //  by items or total size of those items
    this.sendQueue = new LinkedBlockingQueue<>(); // no size yet set
    this.active = true;
  }

  /**
   * Stops the active thread, if one exists.
   */
  public void stopThread() {
    active = false;
  }

  /**
   * Synchronized method to send data out of the DataOutputStream.
   *
   * @param data to send
   * @throws IOException if data can't be sent over the socket
   */
  public synchronized void sendData(byte[] data) throws IOException {
    int length = data.length;
    dout.writeInt(length);
    dout.write(data, 0, length);
    dout.flush();
  }

  /**
   * Appends the sendQueue.
   *
   * @param data to be queued for sending
   * @return true if append successful, false otherwise
   * @throws IOException The active flag has been set to false, implying the
   * sender thread has stopped, or the socket isn't writable.
   */
  public boolean queueSend(byte[] data) throws IOException {
    if (!active) { // notify caller if socket/sender is dead
      throw new IOException("Sender thread is inactive. Socket not writable.");
    }
    try { // attempt to append the sendQueue
      return sendQueue.offer(data, 50, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.debug("Thread interrupted. " + e.getMessage());
      Thread.currentThread().interrupt();
    }
    return false;
  }

  /**
   * Continuously polls the sendQueue and attempts to send messages through the
   * socket after successful polls.
   */
  @Override
  public void run() {
    while (active) {
      try {
        byte[] marshalledBytes = sendQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (marshalledBytes != null) {
          sendData(marshalledBytes);
        }
      } catch (InterruptedException e) {
        logger.debug("Thread interrupted. " + e.getMessage());
        Thread.currentThread().interrupt();
        active = false;
        break;
      } catch (IOException e) {
        logger.debug("Socket connection has closed. " + e.getMessage());
        active = false;
        break;
      }
    }
  }
}
