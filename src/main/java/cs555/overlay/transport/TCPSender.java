package cs555.overlay.transport;
import java.net.SocketTimeoutException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket; 

public class TCPSender {
	
	private Socket socket;
	private DataOutputStream dout;
	private DataInputStream din;

	public TCPSender(Socket socket) throws IOException {
		this.socket = socket;
		this.dout = new DataOutputStream(socket.getOutputStream());
		this.din = new DataInputStream(socket.getInputStream());
	}

	public void sendData(byte[] data) throws IOException {
		int dataLength = data.length;
		dout.writeInt(dataLength);
		dout.write(data, 0, dataLength);
		dout.flush();
	}

	public byte[] receiveData() throws IOException {
		try {
			int dataLength = din.readInt();
			byte[] data = new byte[dataLength];
			din.readFully(data, 0, dataLength);
			return data;
		} catch (SocketTimeoutException ste) {
			return null;
		}
	}

	public void close() {
		try {
			this.socket.close();
		} catch (IOException ioe) {}
		
		try {
			this.dout.close(); 
		} catch (IOException ioe) {}
		
		try {
			this.din.close();
		} catch (IOException ioe) {}
	}
}
