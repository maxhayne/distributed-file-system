package cs555.overlay.wireformats;

import java.io.*;

public class ClientRequestsFileStorageInfo implements Event {

	public byte type;
	public String filename;

	public ClientRequestsFileStorageInfo( String filename ) {
		this.type = Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO;
		this.filename = filename;
	}

	public ClientRequestsFileStorageInfo( byte[] marshalledBytes ) {
		ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

		type = din.readByte();

		int len = din.readInt();
		byte[] array = new byte[len];
		din.readFully( array );
		filename = new String( array );

		din.close();
		bin.close();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( bout );

		dout.write( type );

		byte[] array = filename.getBytes();
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