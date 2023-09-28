package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkServerSendsDeregistration implements Event {

	public int identifier;

	public ChunkServerSendsDeregistration( int identifier ) {
		this.identifier = identifier;
	}

	public ChunkServerSendsDeregistration( byte[] msg ) {
		ByteBuffer buffer = ByteBuffer.wrap( msg );
		buffer.position(1);
		this.identifier = (int) buffer.get();
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( new BufferedOutputStream( baOutputStream ) );

		dout.writeByte(Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION);
		dout.write( (byte) identifier );
		dout.flush();

		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public byte getType() throws IOException {
		return Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION;
	}
}