package cs555.overlay.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitors the number of chunks received in the batch, and if the number of
 * chunks doesn't change for a certain timeout duration, signals the
 * ClientReader either to write what it has received to disk, or to contact more
 * servers to construct the batch.
 *
 * @author hayne
 */
public class NetworkTimer implements Runnable {

  private static final Logger logger = Logger.getInstance();
  private final CyclicBarrier barrier;
  private final AtomicInteger chunksReceivedInBatch;
  private final long timeout; // timeout in ms
  private final long interval; // number of ms to sleep at a time
  private int receivedCheck;
  private boolean cancelled;
  private long elapsed;

  public NetworkTimer(CyclicBarrier barrier,
      AtomicInteger chunksReceivedInBatch) {
    this.barrier = barrier;
    this.chunksReceivedInBatch = chunksReceivedInBatch;
    this.receivedCheck = 0;

    this.cancelled = false;
    this.elapsed = 0;
    this.timeout = 5000; // 5s default
    this.interval = 500; // 500ms default
  }

  public synchronized void cancel() {
    cancelled = true;
  }

  public synchronized void reset() {
    cancelled = false;
    elapsed = 0;
    receivedCheck = 0;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        return;
      }
      synchronized(this) {
        if (cancelled) {
          return; // wrap up this thread
        }
        if (receivedCheck != chunksReceivedInBatch.get()) {
          receivedCheck = chunksReceivedInBatch.get();
          elapsed = 0;
        } else {
          elapsed += interval;
          if (elapsed >= timeout) {
            try {
              barrier.await();
              logger.debug("Network has timed out, returning.");
            } catch (InterruptedException e) {
              logger.debug("Interrupted. " + e.getMessage());
            } catch (BrokenBarrierException e) {
              logger.debug("Broken barrier. " + e.getMessage());
            }
            return;
          }
        }
      }
    }
  }
}