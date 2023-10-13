package cs555.overlay.wireformats;

import java.io.*;

/**
 * For the ChunkServer to send heartbeat information to
 * the Controller.
 * 
 * @author hayne
 */
public class ChunkServerSendsHeartbeat implements Event {

	public byte type;
	public int identifier;
	public int beatType; // 1 is major, 0 is minor
	public int totalChunks;
	public long freeSpace;
	public String[] files;

	public ChunkServerSendsHeartbeat( int identifier, int beatType, 
			int totalChunks, long freeSpace, String[] files ) {
		this.type = Protocol.CHUNK_SERVER_SENDS_HEARTBEAT;
		this.identifier = identifier;
		this.beatType = type;
		this.totalChunks = totalChunks;
		this.freeSpace = freeSpace;
		this.files = files;
	}

	public ChunkServerSendsHeartbeat( byte[] marshalledBytes ) throws IOException {
		ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

		type = din.readByte();

		identifier = din.readInt();

		beatType = din.readInt();

		totalChunks = din.readInt();

		freeSpace = din.readLong();

		int totalFiles = din.readInt();
		if ( totalFiles != 0 ) {
			files = new String[totalFiles];
			for ( int i = 0; i < totalFiles; i++ ) {
				int len = din.readInt();
				byte[] array = new byte[len];
				din.readFully( array );
				files[i] = new String( array );
			}
		}

		bin.close();
		din.close();
	}

	@Override
	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( ( bout ) );
		
		dout.writeByte( type );

		dout.writeInt( identifier );

		dout.writeInt( beatType );

		dout.writeInt( totalChunks );

		dout.writeLong( freeSpace );

		int totalFiles = files == null ? 0 : files.length;
		dout.writeInt (totalFiles );
		for ( int i = 0; i < totalFiles; i++ ) {
			byte[] array = files[i].getBytes();
			dout.writeInt( array.length );
			dout.write( array );
		}

		byte[] marshalledBytes = bout.toByteArray();
		dout.close();
		bout.close();
		return marshalledBytes;
	}

	@Override
	public byte getType() {
		return type;
	}
}