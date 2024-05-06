package cs555.overlay.files;

import cs555.overlay.util.ArrayUtilities;
import cs555.overlay.util.FileMetadata;
import cs555.overlay.util.FileUtilities;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ChunkProcessor {
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

  public boolean repair(FileMetadata md, byte[][] repairSlices,
      int[] repairIndices) throws NoSuchAlgorithmException {
    if (repairIndices != null && repairSlices != null &&
        repairIndices.length == repairSlices.length) {
      for (int i = 0; i < repairIndices.length; ++i) {
        slices[repairIndices[i]] = repairSlices[i];
      }
      updateMetadata(md, repairIndices);
      for (int i = 0; i < 8; ++i) {
        System.arraycopy(slices[i], 0, fileBytes, i*(20 + 8195), 20 + 8195);
      }
      recheckCorruption(repairIndices);
      return true;
    }
    return false;
  }

  private void updateMetadata(FileMetadata md, int[] repairIndices)
      throws NoSuchAlgorithmException {
    md.updateIfWritten(); // update in-memory metadata
    // update metadata in zeroth slice, if it isn't corrupt
    if (ArrayUtilities.contains(repairIndices, 0) ||
        !ArrayUtilities.contains(corruptIndices, 0)) {
      ByteBuffer sliceBuf = ByteBuffer.wrap(slices[0]);
      sliceBuf.putInt(28, md.getVersion());
      sliceBuf.putLong(36, md.getTimestamp());
      byte[] updatedSliceData = new byte[slices[0].length - 20];
      sliceBuf.get(20, updatedSliceData);
      byte[] recomputedHash = FileUtilities.SHA1FromBytes(updatedSliceData);
      sliceBuf.put(0, recomputedHash);
    }
  }

  private void recheckCorruption(int[] repairIndices) {
    ArrayList<Integer> newCorruptions = new ArrayList<>();
    for (int corruptIndex : corruptIndices) {
      if (!ArrayUtilities.contains(repairIndices, corruptIndex)) {
        newCorruptions.add(corruptIndex);
      }
    }
    corruptIndices = ArrayUtilities.listToArray(newCorruptions);
    if (newCorruptions.isEmpty()) {
      corrupt = false;
      content = FileUtilities.getDataFromChunk(
          FileUtilities.removeHashesFromChunk(fileBytes));
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