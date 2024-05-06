package cs555.overlay.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class FileStreamer {
  private static final Logger logger = Logger.getInstance();
  private static final int CACHE_SIZE = 100;
  private static final boolean CACHING = false;
  private final Path serverDirectory;
  private final Deque<Recent> recentFiles;

  private record Recent(Path path, byte[] data) {}

  public FileStreamer(int identifier) {
    this.serverDirectory =
        Paths.get(File.separator, "tmp", "chunk-server-" + identifier);
    this.recentFiles = new ArrayDeque<>();
  }

  // A method to read a file
  public byte[] read(String filename) {
    Path path = serverDirectory.resolve(filename);
    byte[] data = CACHING ? getRecent(path) : null;
    if (data == null) {
      data = readNBytesFromFile(bytesToRead(filename), path);
    }
    if (CACHING) {
      addRecent(path, data);
    }
    return data;
  }

  // A method to write a file
  public boolean write(String filename, byte[] data) {
    Path path = serverDirectory.resolve(filename);
    boolean successful = writeFile(path, data);
    if (CACHING && successful) {
      addRecent(path, data);
    }
    return successful;
  }

  // Deletes a file
  public void delete(String filename) {
    Path path = serverDirectory.resolve(filename);
    deleteFile(path);
    if (CACHING) {
      removeRecent(path);
    }
  }

  public long usableSpace() {
    return (new File(serverDirectory.toString())).getUsableSpace();
  }

  /**
   * Adds a new Recent record to the deque at the front. If the same path is
   * already in the deque, it is removed before the new record is added. The
   * deque is limited to five records.
   *
   * @param path path to file
   * @param data data of file
   */
  private void addRecent(Path path, byte[] data) {
    synchronized(recentFiles) {
      Recent found = null;
      for (Recent recent : recentFiles) {
        if (recent.path().equals(path)) {
          found = recent;
          break;
        }
      }
      if (found != null) {
        recentFiles.remove(found);
      }
      recentFiles.addFirst(new Recent(path, data));
      if (recentFiles.size() > CACHE_SIZE) {
        recentFiles.removeLast();
      }
    }
  }

  // Removes a path from recentFiles
  private void removeRecent(Path path) {
    synchronized(recentFiles) {
      Recent found = null;
      for (Recent recent : recentFiles) {
        if (recent.path().equals(path)) {
          found = recent;
          break;
        }
      }
      if (found != null) {
        recentFiles.remove(found);
      }
    }
  }

  // A method to check recentFiles for the desired file
  private byte[] getRecent(Path path) {
    synchronized(recentFiles) {
      Recent found = null;
      for (Recent recent : recentFiles) {
        if (recent.path().equals(path)) {
          if (recent.data() != null) {
            found = recent;
            break;
          }
        }
      }
      // Copy array and move found to front of recentFiles
      if (found != null) {
        recentFiles.remove(found);
        recentFiles.addFirst(found);
        return Arrays.copyOf(found.data(), found.data().length);
      }
    }
    return null;
  }

  private byte[] readNBytesFromFile(int N, Path path) {
    byte[] fileBytes = new byte[N];
    try (RandomAccessFile file = new RandomAccessFile(path.toString(), "r")) {
      file.read(fileBytes);
    } catch (IOException ioe) {
      logger.debug("Unable to read " + N + " bytes of '" + path + "' " +
                   ioe.getMessage());
    }
    return fileBytes;
  }

  private boolean writeFile(Path path, byte[] data) {
    try {
      Files.createDirectories(serverDirectory);
      Files.write(path, data);
      return true;
    } catch (IOException e) {
      logger.debug("Unable to write " + path + " to disk.");
      return false;
    }
  }

  private void deleteFile(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      logger.debug("Unable to delete " + path + " from disk.");
    }
  }

  private int bytesToRead(String filename) {
    return FilenameUtilities.checkShardFilename(filename) ?
               FileUtilities.SHARD_FILE_LENGTH :
               FileUtilities.CHUNK_FILE_LENGTH;
  }
}