package cs555.overlay.wireformats;

import java.io.*;

/**
 * General Message with variable type and message. Will be
 * used for general purposes.
 * 
 * @author hayne
 */
public class GeneralMessage implements Event {

    private byte type;
    private String message;

    /**
     * Constructor with empty message.
     *
     * @param type of message
     */
    public GeneralMessage( byte type ) {
        this.type = type;
        this.message = "";
    }

    /**
     * Default Constructor with type and non-null message.
     *
     * @param type of message
     * @param message to be sent
     */
    public GeneralMessage( byte type, String message ) {
        this.type = type;

        if ( message == null ) {
            this.message = "";
        } else {
            this.message = message;
        }
    }

    public GeneralMessage( byte[] marshalledBytes ) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

        type = din.readByte();

        short len = din.readShort();
        if ( len != 0 ) {
            byte[] messageBytes = new byte[len];
            din.readFully( messageBytes );
            message = new String( messageBytes );
        } else {
            message = "";
        }

        din.close();
        bin.close();
    }

    public String getMessage() { 
        return message;
    }

    @Override
    public byte getType() { 
        return type;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream( bout );

        dout.write( type );

        if ( message.length() != 0 ) {
            byte[] messageBytes = message.getBytes();
            dout.writeShort( messageBytes.length );
            dout.write( messageBytes );
        } else {
            dout.writeShort( 0 );
        }

        byte[] returnable = bout.toByteArray();
        dout.close();
        bout.close();
        return returnable;
    }
}