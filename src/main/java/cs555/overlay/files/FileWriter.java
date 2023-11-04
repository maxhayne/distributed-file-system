package cs555.overlay.files;


import cs555.overlay.util.FileSynchronizer;

import java.security.NoSuchAlgorithmException;

/**
 * An interface that both ChunkWriter and ShardWriter implement. This way, files
 * can be written to disk without explicitly stating whether they are chunks or
 * shards, which is useful for generalizing messages received by the
 * ChunkServer.
 *
 * @author hayne
 */
public interface FileWriter {
  /**
   * Returns the filename associated with the FileWriter.
   *
   * @return filename associated with this FileWriter
   */
  String getFilename();

  /**
   * Set the content of the writer.
   *
   * @param content byte[] of file content
   */
  void setContent(byte[] content);

  /**
   * Fully prepares the byte string that represents the file to be written.
   *
   * @throws NoSuchAlgorithmException if SHA1 can't be used
   */
  void prepare() throws NoSuchAlgorithmException;

  /**
   * Writes the prepared byte string to disk with the filename associated with
   * this FileWriter.
   *
   * @param synchronizer the ChunkServer is using to synchronize file accesses
   * across threads
   * @return true if the file was written successfully, false otherwise
   */
  boolean write(FileSynchronizer synchronizer);

}
