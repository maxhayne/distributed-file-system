package cs555.overlay.util;

public abstract class ServerFile {
	public String filename;
	public int sequence;
	public abstract String getType();
}