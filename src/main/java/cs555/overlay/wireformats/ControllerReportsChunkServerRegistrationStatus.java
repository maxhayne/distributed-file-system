package cs555.overlay.wireformats;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ControllerReportsChunkServerRegistrationStatus implements Event {

	public int status;

	public ControllerReportsChunkServerRegistrationStatus( int status ) {
		this.status = (byte) status;
	}

	public ControllerReportsChunkServerRegistrationStatus( byte[] msg ) {
		ByteBuffer buffer = ByteBuffer.wrap(msg);
		buffer.position(1);
		this.status = (int) buffer.get();
	}

	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( new BufferedOutputStream( baOutputStream ) );

		dout.writeByte( Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS );
		dout.write( (byte) status );

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();

		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public byte getType() throws IOException {
		return Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS;
	}
}