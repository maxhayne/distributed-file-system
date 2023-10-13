package cs555.overlay.transport;

import cs555.overlay.node.Node;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class TCPConnectionCache {

    private final Map<String, TCPConnection> cachedConnections;

    public TCPConnectionCache() {
        this.cachedConnections = new HashMap<>();
    }

    /**
     * Returns a TCPConnection connected to the address specified as a
     * parameter. If a TCPConnection with the specified address already exists
     * in cachedConnections, it is returned, otherwise, a new connection is
     * created, and is added to cachedConnections before being returned.
     *
     * @param node that connection's events will be processed in
     * @param address host:port string
     * @return TCPConnection to specified host:port
     * @throws IOException if connection can't be created
     */
    public TCPConnection getConnection( Node node, String address,
        boolean start ) throws IOException {
        TCPConnection connection;
        synchronized ( cachedConnections ) {
            if ( cachedConnections.containsKey( address ) ) {
                connection = cachedConnections.get( address );
            } else {
                connection = establishConnection( node, address );
                cachedConnections.put( address, connection );
                if ( start ) {
                    connection.start();
                }
            }
        }
        return connection;
    }

    /**
     * Establish a TCPConnection connected to the host:port address specified as
     * a parameter.
     *
     * @param node that connection's events will be processed in
     * @param address host:port string
     * @return TCPConnection to specified host:port
     * @throws IOException if socket can't be created
     * @throws NumberFormatException if provided address isn't formatted
     * correctly, if port in host:port is not a number
     */
    public static TCPConnection establishConnection( Node node, String address )
        throws IOException, NumberFormatException {
        Socket socket = new Socket( address.split( ":" )[0],
            Integer.parseInt( address.split( ":" )[1] ) );
        return new TCPConnection( node, socket );
    }
}
