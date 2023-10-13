package cs555.overlay.wireformats;

import java.io.*;

public class RequestsSlices implements Event {

    public byte type;
    public String filename;
    public int[] slices;

    public RequestsSlices( String filename, int[] slices ) {
        this.type = Protocol.REQUESTS_SLICES;
        this.filename = filename;
        this.slices = slices;
    }

    public RequestsSlices( byte[] marshalledBytes ) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

        type = din.readByte();

        int len = din.readInt();
        byte[] array = new byte[len];
        din.readFully( array );
        filename = new String( array );

        int slicesLength = din.readInt();
        slices = new int[slicesLength];
        for ( int i = 0; i < slicesLength; ++i ) {
            slices[i] = din.readInt();
        }

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

        dout.writeInt( slices.length );
        for ( int i = 0; i < slices.length; ++i ) {
            dout.writeInt( slices[i] );
        }

        byte[] marshalledBytes = bout.toByteArray();
        dout.close();
        bout.close();
        return marshalledBytes;
    }

    public byte getType() {
        return type;
    }
}