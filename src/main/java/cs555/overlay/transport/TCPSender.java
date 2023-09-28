package cs555.overlay.transport;

import java.net.SocketTimeoutException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket; 

/**
 * Simple class to send out of the DataOutputStream
 * associated with the Socket passed in the constructor.
 * 
 * @author hayne
 */
public class TCPSender {
	
	protected DataOutputStream dout;

	/**
	 * Creates DataOutputStream based on the passed Socket.
	 * @param socket
	 * @throws IOException
	 */	
	public TCPSender( Socket socket ) throws IOException {
		this.dout = new DataOutputStream( socket.getOutputStream() );
	}

	/**
	 * Synchronized method to send data out of the DataOutputStream.
	 * @param data to send
	 * @throws IOException
	 */
	public synchronized void sendData( byte[] data ) throws IOException {
		int length = data.length;
		dout.writeInt( length );
		dout.write( data, 0, length );
		dout.flush();
	}
}
