package cs555.overlay.wireformats;

import java.io.*;

public class ChunkServerReportsFileCorruption implements Event {

	public byte type;
	public int identifier;
	public String filename;
	public int[] slices;

	public ChunkServerReportsFileCorruption( int identifier, String filename, int[] slices ) {
		this.type = Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION;
		this.identifier = identifier;
		this.filename = filename;
		this.slices = slices;
	}

	public ChunkServerReportsFileCorruption( byte[] marshalledBytes ) {
		ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

		type = din.readByte();

		identifier = din.readInt();

		int len = din.readInt();
		byte[] array = new byte[len];
		din.readFully( array );
		filename = new String( array );

		int numSlices = din.readInt();
		if ( numSlices > 0 ) {
			slices = new int[numSlices];
			for ( int i = 0; i < numSlices; ++i ) {
				slices[i] = din.readInt();
			}
		} else {
			slices = null;
		}

		din.close();
		bin.close();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( bout );

		dout.write( type );

		dout.writeInt( identifier );

		byte[] array = filename.getBytes();
		dout.writeInt( array.length );
		dout.write( array );

		if ( slices == null ) {
			dout.writeInt( 0 );
		} else {
			dout.writeInt( slices.length );
			for ( int i = 0; i < slices.length; ++i ) {
				dout.writeInt( slices[i] );
			}
		}

		byte[] returnable = bout.toByteArray();
        dout.close();
        bout.close();
        return returnable;
	}

	public byte getType() throws IOException {
		return type;
	}
}