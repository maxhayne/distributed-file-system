package cs555.overlay.files;

import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileUtilities;
import cs555.overlay.wireformats.RepairChunk;
import cs555.overlay.wireformats.RequestChunk;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ChunkProcessor implements FileProcessor {
  private final byte[] fileBytes;
  private byte[] content;
  private boolean corrupt;

  private final byte[][] slices;
  private int[] corruptIndices;

  public ChunkProcessor(byte[] fileBytes) throws NoSuchAlgorithmException {
    this.fileBytes = fileBytes;
    this.slices = splitToSlices(fileBytes);
    List<Integer> corruptions =
        FileUtilities.checkChunkForCorruption(fileBytes);
    if (corruptions.isEmpty()) {
      this.corrupt = false;
      this.content = FileUtilities.getDataFromChunk(
          FileUtilities.removeHashesFromChunk(fileBytes));
    } else {
      this.corrupt = true;
      this.corruptIndices = ArrayUtilities.listToArray(corruptions);
    }
  }

  private byte[][] splitToSlices(byte[] fileBytes) {
    byte[][] slices = new byte[8][20 + 8195];
    for (int i = 0; i < 8; ++i) {
      System.arraycopy(fileBytes, i*(20 + 8195), slices[i], 0, 20 + 8195);
    }
    return slices;
  }

  public boolean repair(FileMetadata md, byte[][] repairSlices)
      throws NoSuchAlgorithmException {
    if (!corrupt) {
      return false; // repair not necessary
    }
    if (repairSlices != null && repairSlices.length == slices.length) {
      boolean repaired = false;
      for (int i = 0; i < slices.length; ++i) {
        if (ArrayUtilities.contains(corruptIndices, i) &&
            repairSlices[i] != null) {
          slices[i] = repairSlices[i];
          repaired = true;
        }
      }
      if (repaired) {
        updateMetadata(md, repairSlices);
        for (int i = 0; i < 8; ++i) {
          System.arraycopy(slices[i], 0, fileBytes, i*(20 + 8195), 20 + 8195);
        }
        recheckCorruption(repairSlices);
        return true;
      }
    }
    return false;
  }

  private void updateMetadata(FileMetadata md, byte[][] repairSlices)
      throws NoSuchAlgorithmException {
    md.updateIfWritten(); // update in-memory metadata
    // update metadata in zeroth slice, if it isn't corrupt
    if (!ArrayUtilities.contains(corruptIndices, 0) ||
        repairSlices[0] != null) {
      ByteBuffer sliceBuf = ByteBuffer.wrap(slices[0]);
      sliceBuf.putInt(28, md.getVersion());
      sliceBuf.putLong(36, md.getTimestamp());
      byte[] updatedSliceData = new byte[slices[0].length - 20];
      sliceBuf.get(20, updatedSliceData);
      byte[] recomputedHash = FileUtilities.SHA1FromBytes(updatedSliceData);
      sliceBuf.put(0, recomputedHash);
    }
  }

  private void recheckCorruption(byte[][] repairSlices) {
    ArrayList<Integer> corruptions = new ArrayList<>();
    for (int corruptIndex : corruptIndices) {
      if (repairSlices[corruptIndex] == null) {
        corruptions.add(corruptIndex);
      }
    }
    corruptIndices = ArrayUtilities.listToArray(corruptions);
    if (corruptions.isEmpty()) {
      corrupt = false;
      content = FileUtilities.getDataFromChunk(
          FileUtilities.removeHashesFromChunk(fileBytes));
    }
  }

  /**
   * Attach all the slices to the message that aren't corrupted.
   *
   * @param request RequestChunk message
   */
  public void attachToRequest(RequestChunk request) {
    for (int i = 0; i < slices.length; ++i) {
      if (!ArrayUtilities.contains(corruptIndices, i)) {
        request.getPieces()[i] = slices[i];
      }
    }
  }

  /**
   * Attach all the slices to the message that aren't corrupted, and that the
   * message is slotted to replace.
   *
   * @param repair RepairChunk message
   */
  public void attachToRepair(RepairChunk repair) {
    int[] piecesToRepair = repair.getPiecesToRepair();
    for (int i = 0; i < slices.length; ++i) {
      if (!ArrayUtilities.contains(corruptIndices, i) &&
          ArrayUtilities.contains(piecesToRepair, i)) {
        repair.getPieces()[i] = slices[i];
      }
    }
  }

  public byte[] getBytes() {
    return fileBytes;
  }

  public byte[] getContent() {
    return content;
  }

  public boolean isCorrupt() {
    return corrupt;
  }

  public int[] getCorruptIndices() {
    return corruptIndices;
  }

  public byte[][] getSlices() {
    return slices;
  }
}