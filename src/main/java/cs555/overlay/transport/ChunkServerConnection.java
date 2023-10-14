package cs555.overlay.transport;

import cs555.overlay.util.HeartbeatInfo;

import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Maintains an active connection with a ChunkServer. Its function is to
 * maintain status information and to actively send messages in its queue to the
 * ChunkServer it's connected with.
 */
public class ChunkServerConnection implements Runnable {

    private final String host;
    private final int port;
    private final TCPConnection connection;

    // Identifying/Connection information
    private final int identifier;
    private final BlockingQueue<byte[]> sendQueue;

    // Status information
    private final long startTime;
    private int unhealthy;
    private boolean activeStatus;
    private final HeartbeatInfo heartbeatInfo; // latest heartbeat info
    private int pokes;
    private int pokeReplies;

    public ChunkServerConnection( int identifier, String host, int port,
        TCPConnection connection ) {
        this.identifier = identifier;
        this.host = host;
        this.port = port;
        this.connection = connection;
        this.sendQueue =
            new LinkedBlockingQueue<byte[]>(); // useful for TCPSender?

        this.startTime = System.currentTimeMillis();
        this.unhealthy = 0;
        this.activeStatus = false;
        this.heartbeatInfo = new HeartbeatInfo();
        this.pokes = 0;
        this.pokeReplies = 0;
    }

    public synchronized void incrementPokes() {
        pokes++;
    }

    public synchronized void incrementPokeReplies() {
        pokeReplies++;
    }

    public synchronized int getPokeDiscrepancy() {
        return ( pokes - pokeReplies );
    }

    public HeartbeatInfo getHeartbeatInfo() {
        return heartbeatInfo;
    }

    public synchronized long getStartTime() {
        return startTime;
    }

    public int getIdentifier() {
        return identifier;
    }

    public String getLocalAddress() {
        return connection.getSocket().getLocalAddress().getHostAddress();
    }

    public String getRemoteAddress() {
        return connection.getSocket().getInetAddress().getHostAddress();
    }

    public int getRemotePort() {
        return connection.getSocket().getPort();
    }

    public String getServerAddress() {
        return host;
    }

    public int getServerPort() {
        return port;
    }

    public synchronized int getUnhealthy() {
        return unhealthy;
    }

    public synchronized void incrementUnhealthy() {
        this.unhealthy += 1;
    }

    public synchronized void decrementUnhealthy() {
        if ( unhealthy > 0 ) {
            unhealthy -= 1;
        }
    }

    public synchronized boolean getActiveStatus() {
        return activeStatus;
    }

    public synchronized void setActiveStatus( boolean status ) {
        activeStatus = status;
    }

    public TCPConnection getConnection() {
        return connection;
    }

    public synchronized String print() {
        String remoteAddress = "Unknown";
        int remotePort = -1;
        try {
            remoteAddress = getRemoteAddress();
            remotePort = getRemotePort();
        } catch ( Exception e ) {
            // If we can't get the remote address and port,
            // we'll just print "Unknown" and "-1".
        }

        String activeString = activeStatus ? "active" : "inactive";
        String returnable =
            "[ " + identifier + ", " + remoteAddress + ":" + remotePort + ", "
            + heartbeatInfo.getFreeSpace() + ", " + activeString + ", health:"
            + unhealthy + " ]\n";
        return returnable;
    }

    // use to send to this chunk server
    public boolean addToSendQueue( byte[] msg ) {
        try {
            this.sendQueue.put( msg );
        } catch ( InterruptedException ie ) {
            return false;
        }
        return true;
    }

    @Override
    public void run() {
        this.setActiveStatus( true );
        while ( this.getActiveStatus() ) {
            byte[] msg = null;
            try {
                msg = this.sendQueue.poll( 1, TimeUnit.SECONDS );
            } catch ( InterruptedException ie ) {
                System.err.println(
                    "ChunkServerConnection::run InterruptedException: "
                    + ie.getMessage() );
            }
            if ( msg != null ) {
                try {
                    connection.getSender()
                        .sendData( msg ); // send the message along
                } catch ( Exception e ) {
                    System.err.println( "ChunkServerConnection::run Exception: "
                                        + e.getMessage() );
                }
            }
        }
    }
}