package cs555.overlay.wireformats;

import java.io.*;

public class ChunkServerNoStoreFile {

	public byte type;
	public String address; // Controller performs lookup to find the identifier of server that failed to store the file
	public String filename;

	public ChunkServerNoStoreFile( String address, String filename ) {
		this.type = Protocol.CHUNK_SERVER_NO_STORE_FILE;
		this.address = address;
		this.filename = filename;
	}

	public ChunkServerNoStoreFile( byte[] marshalledBytes ) {
		ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

		type = din.readByte();

		int len = din.readInt();
		byte[] array = new byte[len];
		din.readFully( array );
		address = new String( array );

		len = din.readInt();
		array = new byte[len];
		din.readFully( array );
		filename = new String( array );

		din.close();
		bin.close();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( bout );

		dout.write( type );

		byte[] array = address.getBytes();
		dout.writeInt( array.length );
		dout.write( array );

		array = filename.getBytes();
		dout.writeInt( array.length );
		dout.write( array );

		byte[] returnable = bout.toByteArray();
        dout.close();
        bout.close();
        return returnable;
 	}

 	public byte getType() throws IOException {
 		return type;
 	}
 }