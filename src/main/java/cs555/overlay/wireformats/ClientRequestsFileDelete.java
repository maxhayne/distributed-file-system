package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ClientRequestsFileDelete implements Event {

	public byte type;
	public String filename;

	public ClientRequestsFileDelete( String filename ) {
		this.type = Protocol.CLIENT_REQUESTS_FILE_DELETE;
		this.filename = filename;
	}

	public ClientRequestsFileDelete( byte[] msg ) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);

		int length = buffer.getInt();
		byte[] array = new byte[length];
		buffer.get(array);
		this.filename = new String(array);
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( new BufferedOutputStream( baOutputStream ) );

		dout.writeByte(Protocol.CLIENT_REQUESTS_FILE_DELETE);
		
		byte[] array = filename.getBytes();
		dout.writeInt( array.length );
		dout.write( array );
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	@Override
	public byte getType() throws IOException {
		return type;
	}
}