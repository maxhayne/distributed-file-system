package cs555.overlay.util;

/**
 * Interface to store constants that will be used both
 * by ChunkServers and the Client when working with files.
 */
public class Constants {
    public static final int DATA_SHARDS = 6; 
	public static final int PARITY_SHARDS = 3; 
	public static final int TOTAL_SHARDS = 9;
	public static final int BYTES_IN_INT = 4;
	public static final int BYTES_IN_LONG = 8;
    public static final int HEARTRATE = 15000;
}