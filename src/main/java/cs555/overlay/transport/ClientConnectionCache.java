package cs555.overlay.transport;
import java.util.Collections;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map;

public class ClientConnectionCache {

	private Map<Integer,ClientConnection> clientcache;
	private Vector<Integer> identifierList;

	public ClientConnectionCache() {
		this.clientcache = new TreeMap<Integer,ClientConnection>();
		this.identifierList = new Vector<Integer>();
		for (int i = 0; i < 8; i++)
			this.identifierList.add(i);
		Collections.shuffle(this.identifierList);
	}

	public ClientConnection register(TCPReceiverThread tcpreceiverthread) throws IOException {
		int identifier;
		synchronized(identifierList) {
			if (identifierList.size() > 0) {
				identifier = identifierList.remove(identifierList.size()-1);
			} else {
				return null;
			}
		}
		ClientConnection newConnection = new ClientConnection(tcpreceiverthread,identifier);
		synchronized(clientcache) {
			clientcache.put(identifier,newConnection);
			newConnection.start();
		}
		return newConnection;
	}

	// Not much matters if a client wants to disconnect
	public boolean deregister(int identifier) {
		synchronized(clientcache) {
			if (clientcache.containsKey(identifier)) {
				clientcache.remove(identifier);
			} else {
				return false;
			}
		}
		synchronized(identifierList) {
			identifierList.add(identifier);
		}
		return true;
	}
}