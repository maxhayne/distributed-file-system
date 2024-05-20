package cs555.overlay.util;

import cs555.overlay.node.Client;
import cs555.overlay.transport.TCPConnectionCache;
import cs555.overlay.wireformats.Event;
import cs555.overlay.wireformats.GeneralMessage;
import cs555.overlay.wireformats.Protocol;
import cs555.overlay.wireformats.RequestChunk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class used by the Client to read the chunks of a file from the DFS and
 * write it to disk locally.
 *
 * @author hayne
 */
public class ClientReader implements Runnable {

  private static final Logger logger = Logger.getInstance();
  private static final int BATCH_SIZE = 1024; // chunks per batch
  private final Client client;
  private final String filename;
  private final AtomicInteger totalChunksReceived;
  private final AtomicInteger totalChunks;
  private final TCPConnectionCache connectionCache;
  private final Path readDirectory;
  private final AtomicInteger batchStartIndex;
  private final AtomicInteger chunksReceivedInBatch;
  private final CyclicBarrier batchBarrier;
  private String[][] servers; // set by Controller response
  private byte[][] receivedChunks; // set by Controller response
  private volatile boolean stopRequested;

  /**
   * Constructor. Creates a new ClientReader which can be passed to a new thread
   * to assemble chunks stored on the DFS into a file 'filename' to write to
   * disk.
   *
   * @param client Client on which the ClientReader will be executing
   * @param filename filename of file to read from the DFS and write to disk
   */
  public ClientReader(Client client, String filename) {
    this.client = client;
    this.filename = filename;
    this.totalChunksReceived = new AtomicInteger(0);
    this.totalChunks = new AtomicInteger(0);
    this.connectionCache = new TCPConnectionCache(client);
    this.readDirectory = Paths.get(System.getProperty("user.dir"), "reads");
    this.stopRequested = false;
    this.batchStartIndex = new AtomicInteger(0);
    this.chunksReceivedInBatch = new AtomicInteger(0);
    this.batchBarrier = new CyclicBarrier(2);
  }

  /**
   * Downloads the file and writes it to disk.
   */
  @Override
  public synchronized void run() {
    if (createReadDirectory()) {
      try (RandomAccessFile file = new RandomAccessFile(
          readDirectory.resolve(filename).toString(), "rw");
           FileChannel channel = file.getChannel();
           FileLock fileLock = channel.lock()) {
        if (getStorageInfo()) { // We've retrieved the storage info
          initializeReceivedFiles();
          setTotalChunks();
          channel.truncate(0); // truncate the file if it exists
          batchedDownloadAndWrite(file);
        }
      } catch (IOException e) {
        logger.error(
            "Couldn't write '" + filename + "' to disk. " + e.getMessage());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Thread interrupted.");
      }
    }
    try {
      cleanup();
    } catch (InterruptedException e) {
      logger.error(filename + " cleanup() interrupted.");
    }
  }

  /**
   * Downloads BATCH_SIZE chunks at a time and writes them to disk repeatedly
   * until the file has been retrieved.
   *
   * @param file to be written to
   * @throws IOException if writing to file fails
   */
  private void batchedDownloadAndWrite(RandomAccessFile file)
      throws IOException, InterruptedException {
    int numberOfBatches = 1 + (servers.length/BATCH_SIZE);
    int batch = 0;
    while (batch < numberOfBatches && !stopRequested) {
      batchStartIndex.set(batch*BATCH_SIZE);
      freePreviousBatch();
      wrangleChunks();
      writeChunksToDisk(file);
      logger.debug("Batch " + batch + " was written to disk.");
      batch++;
    }
  }

  /**
   * Frees up references to chunks or shards in the previous batch so that they
   * may be garbage collected.
   */
  private void freePreviousBatch() {
    int workBackwardsFrom = batchStartIndex.get() - 1;
    for (int i = workBackwardsFrom;
         i >= 0 && i > workBackwardsFrom - BATCH_SIZE; --i) {
      synchronized(servers[i]) { // Probably don't need this
        receivedChunks[i] = null;
      }
    }
  }

  /**
   * Creates total chunks based on the number of available slots in
   * receivedFiles.
   */
  private void setTotalChunks() {
    totalChunks.set(receivedChunks.length);
  }

  /**
   * Creates a new byte[][][] receivedFiles based on the length of the servers
   * array.
   */
  private void initializeReceivedFiles() {
    receivedChunks = new byte[servers.length][];
  }

  /**
   * Adds a byte[] to the receivedFiles[][] array. Will be called through the
   * Client's onEvent method by TCPReceiverThreads that have received messages
   * from ChunkServers. If the file being added is not part of the current
   * batch, it is ignored.
   *
   * @param filename contains "_chunk#"
   * @param content byte[] of chunk's content
   */
  public void addFile(String filename, byte[] content) {
    int sequence = FilenameUtilities.getSequence(filename);
    if (sequence >= batchStartIndex.get()) { // only add if in batch
      synchronized(servers[sequence]) {
        receivedChunks[sequence] = content;
        chunksReceivedInBatch.incrementAndGet(); // chunk received
        totalChunksReceived.incrementAndGet();
      }
      // If the batch has been fully downloaded, await() on batchBarrier
      // so that thread in wrangleChunks can proceed, and write to disk
      if (chunksReceivedInBatch.get() >= chunksToGetInBatch()) {
        if (!batchBarrier.isBroken()) {
          try {
            batchBarrier.await(); // proceed thread in wrangleChunks
          } catch (InterruptedException|BrokenBarrierException e) {
            logger.debug("Barrier exception. " + e.getMessage());
          }
        }
      }
    }
  }

  /**
   * Writes all the chunks present in the receivedFiles array to disk
   * sequentially. If erasure coding, the chunk is decoded from the fragments
   * first, of course.
   *
   * @param file file opened in the run method
   * @throws IOException if error encountered while writing
   */
  private void writeChunksToDisk(RandomAccessFile file) throws IOException {
    int nullChunks = 0;
    int localBatchStartIndex = batchStartIndex.get();
    for (int i = localBatchStartIndex; i < localBatchStartIndex + BATCH_SIZE &&
                                       i < receivedChunks.length; ++i) {
      synchronized(servers[i]) {
        if (receivedChunks[i] != null) {
          file.write(receivedChunks[i]);
        } else {
          ++nullChunks;
        }
      }
    }
    if (nullChunks > 0) {
      logger.info(nullChunks + " chunks were missing from batch " +
                  localBatchStartIndex/BATCH_SIZE + " for " + filename);
    }
  }

  /**
   * Attempts to download and write to disk all the chunks for the current
   * batch.
   */
  private void wrangleChunks() throws InterruptedException {
    chunksReceivedInBatch.set(0); // haven't received any chunks for this batch
    NetworkTimer timer = new NetworkTimer(batchBarrier, chunksReceivedInBatch);
    batchBarrier.reset(); // make sure it's set to two
    requestBatch();
    timer.reset();
    Thread timerThread = new Thread(timer);
    timerThread.start(); // start network timer
    try {
      batchBarrier.await(); // triggered by timeout or complete download
    } catch (BrokenBarrierException bbe) {
      logger.debug("Barrier broken. " + bbe.getMessage());
    }
    timerThread.interrupt();
    timer.cancel(); // if interrupt hasn't stopped it
    timerThread.join();
  }

  /**
   * Returns the total number of chunks to download in this batch. Will only
   * differ from BATCH_SIZE for the last batch.
   *
   * @return number of chunks to get this batch
   */
  private int chunksToGetInBatch() {
    int localBatchStartIndex = batchStartIndex.get();
    int batchEndIndex =
        Math.min(localBatchStartIndex + BATCH_SIZE, receivedChunks.length);
    return batchEndIndex - localBatchStartIndex;
  }

  /**
   * Sends chunk requests to servers for the current batch.
   */
  private void requestBatch() {
    int localBatchStartIndex = batchStartIndex.get();
    for (int i = localBatchStartIndex;
         i < localBatchStartIndex + BATCH_SIZE && i < servers.length; ++i) {
      requestChunk(i, servers[i]);
    }
  }

  /**
   * Sends out a chunk request message.
   *
   * @param servers servers the chunk is available at
   * @param sequence number of chunk
   * @return true if request message was sent, false otherwise
   */
  private boolean requestChunk(int sequence, String[] servers) {
    String chunkFilename = filename + "_chunk" + sequence;
    String address = client.getHost() + ":" + client.getPort();
    RequestChunk request = new RequestChunk(chunkFilename, servers, address);
    String nextServer = request.getAddress();
    do {
      if (connectionCache.send(nextServer, request, false, true)) {
        return true;
      }
    } while ((nextServer = request.getNextAddress()) != null);
    logger.debug(chunkFilename + " could not be requested.");
    return false;
  }

  /**
   * Cleans up this ClientReader.
   */
  private void cleanup() throws InterruptedException {
    client.removeReader(filename); // remove self
    Thread.sleep(1000);
    connectionCache.closeConnections(); // shutdown connections
    logger.info("The ClientReader for " + filename + " has cleaned up.");
  }

  /**
   * Create the directory into which the file will be stored.
   *
   * @return true if directory created, false otherwise
   */
  private boolean createReadDirectory() {
    try {
      Files.createDirectories(readDirectory);
      return true;
    } catch (IOException e) {
      logger.error("Couldn't make directory to store files from the DFS. " +
                   readDirectory);
      return false;
    }
  }

  /**
   * Name is self-explanatory. Will be called from the Client when the
   * Controller has responded to a file storage info request.
   *
   * @param servers what to set 'servers' to
   */
  public synchronized void setServersAndNotify(String[][] servers) {
    this.servers = servers;
    this.notify();
  }

  /**
   * Sends a message to the Controller asking for the storage information for
   * the filename given to this ClientReader in the constructor. Waits to be
   * unlocked, which will either happen when a response arrives from the
   * Controller, or when the user issues an interruption.
   *
   * @return the total number of chunks stored on the DFS,
   */
  private synchronized boolean getStorageInfo() {
    byte type = Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO;
    GeneralMessage storageInfoMessage = new GeneralMessage(type, filename);
    if (sendToController(storageInfoMessage)) {
      waitForStorageInfo();
    }
    if (servers != null && !stopRequested) {
      return true;
    } else {
      if (stopRequested) {
        logger.debug("Reader for " + filename + " stopped by user.");
      } else {
        logger.error(filename + " has zero chunks stored on the DFS.");
      }
      return false;
    }
  }

  /**
   * Waits for storage info from the Controller to arrive. If the storage info
   * arrives, but is null, requestStop() is called, guaranteeing this function
   * will return.
   */
  private void waitForStorageInfo() {
    while (servers == null && !stopRequested) {
      try {
        this.wait(5000);
      } catch (InterruptedException e) {
        logger.debug(e.getMessage());
      }
    }
  }

  /**
   * Sets stopRequested to true;
   */
  public void requestStop() {
    stopRequested = true;
  }

  /**
   * Sends a message to the Controller.
   *
   * @param event message to send
   * @return true if sent, false if not
   */
  private boolean sendToController(Event event) {
    try {
      client.getControllerConnection().getSender().sendData(event.getBytes());
    } catch (IOException e) {
      System.err.println("Couldn't send to Controller." + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Returns a number between 0 and 100 representing the progress in retrieving
   * the chunks of the file from the DFS.
   *
   * @return percentage of files retrieved from DFS
   */
  public int getProgress() {
    return totalChunks.get() == 0 ? 0 : (int) (
        ((double) totalChunksReceived.get()/(double) totalChunks.get())*100.0);
  }
}