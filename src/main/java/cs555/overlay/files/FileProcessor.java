package cs555.overlay.files;

import cs555.overlay.util.FileMetadata;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RequestChunk;

import java.security.NoSuchAlgorithmException;

public interface FileProcessor {
  boolean repair(FileMetadata md, byte[][] repairPieces)
      throws NoSuchAlgorithmException;

  void attachToRequest(RequestChunk request);

  void attachToRepair(RepairChunk repair);

  byte[] getBytes();

  boolean isCorrupt();
}
