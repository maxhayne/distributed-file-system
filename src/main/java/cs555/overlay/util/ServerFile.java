package cs555.overlay.util;

public abstract class ServerFile {
  public String filename;
  public int sequence;
  public int version;
  public long timestamp;
  public int serverIdentifier;
  public boolean corrupt;

  public abstract byte getType();
}