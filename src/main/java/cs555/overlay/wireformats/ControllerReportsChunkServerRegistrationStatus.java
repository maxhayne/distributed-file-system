package cs555.overlay.wireformats;

import java.io.*;

public class ControllerReportsChunkServerRegistrationStatus implements Event {

    public byte type;
    public int status;

    public ControllerReportsChunkServerRegistrationStatus( int status ) {
        this.type =
                Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS;
        this.status = status;
    }

    public ControllerReportsChunkServerRegistrationStatus(
            byte[] marshalledBytes ) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

        type = din.readByte();

        status = din.readInt();

        din.close();
        bin.close();
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream( bout );

        dout.write( type );

        dout.writeInt( status );

        byte[] returnable = bout.toByteArray();
        dout.close();
        bout.close();
        return returnable;
    }

    public byte getType() {
        return type;
    }
}