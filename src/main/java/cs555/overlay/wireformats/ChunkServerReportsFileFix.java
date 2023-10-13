package cs555.overlay.wireformats;

import java.io.*;

public class ChunkServerReportsFileFix implements Event {

	public byte type;
	public int identifier;
	public String filename;

	public ChunkServerReportsFileFix( int identifier, String filename ) {
		this.type = Protocol.CHUNK_SERVER_REPORTS_FILE_FIX;
		this.identifier = identifier;
		this.filename = filename;
	}

	public ChunkServerReportsFileFix( byte[] marshalledBytes ) throws IOException {
		ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

		type = din.readByte();

		identifier = din.readInt();

		int len = din.readInt();
		byte[] array = new byte[len];
		din.readFully( array );
		filename = new String( array );

		din.close();
		bin.close();
	}

	@Override
	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( bout );

		dout.write( type );

		dout.writeInt( identifier );

		byte[] array = filename.getBytes();
		dout.writeInt( array.length );
		dout.write( array );

		byte[] returnable = bout.toByteArray();
        dout.close();
        bout.close();
        return returnable;
	}

	@Override
	public byte getType() {
		return type;
	}
}