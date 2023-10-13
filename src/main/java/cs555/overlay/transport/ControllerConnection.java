package cs555.overlay.transport;

import cs555.overlay.util.ChunkServerHeartbeatService;
import cs555.overlay.util.Constants;
import cs555.overlay.util.FileDistributionService;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ControllerConnection implements Runnable {

    private TCPConnection connection;
    private BlockingQueue<byte[]> sendQueue;
    private boolean activeStatus;
    private ChunkServerHeartbeatService heartbeatService;
    private FileDistributionService fileService;
    private Timer heartbeatTimer;
    private int serverIdentifier;

    public ControllerConnection( TCPConnection connection ) throws IOException {
        this.connection = connection;
        this.fileService = new FileDistributionService( directory, this );
        this.activeStatus = false;
        this.sendQueue = new LinkedBlockingQueue<byte[]>();
        this.fileService.start();
        this.serverIdentifier = -1;
    }

    public String getServerAddress() throws UnknownHostException {
        return this.receiver.getLocalAddress();
    }

    public int getServerPort() {
        return this.server.getLocalPort();
    }

    public synchronized void setIdentifier( int identifier ) {
        this.serverIdentifier = identifier;
    }

    public synchronized int getIdentifier() {
        return this.serverIdentifier;
    }

    public synchronized void startHeartbeatService() {
        this.heartbeatService = new ChunkServerHeartbeatService( this.directory, this,
                this.fileService );
        this.heartbeatTimer = new Timer();
        long randomOffset = ( long ) ThreadLocalRandom.current().nextInt( 2, 15 + 1 );
        long heartbeatStart = randomOffset * 1000L;
        this.heartbeatTimer.scheduleAtFixedRate( heartbeatService, heartbeatStart,
                Constants.HEARTRATE );
    }

    public synchronized void stopHeartbeatService() {
        if ( heartbeatTimer != null ) {
            heartbeatTimer.cancel();
        }
        heartbeatTimer = null;
    }

    public synchronized boolean getActiveStatus() {
        return activeStatus;
    }

    public synchronized void setActiveStatus( boolean status ) {
        activeStatus = status;
    }

    public boolean addToSendQueue( byte[] msg ) {
        try {
            this.sendQueue.put( msg );
        } catch ( InterruptedException ie ) {
            return false;
        }
        return true;
    }

    public void shutdown() {
        this.stopHeartbeatService();
        this.server.close();
        this.receiver.close();
        this.setActiveStatus( false );
    }

    public synchronized void sendData( byte[] data ) throws IOException {
        int length = data.length;
        dout.writeInt( length );
        dout.write( data, 0, length );
        dout.flush();
    }

    @Override
    public void run() {
        this.setActiveStatus( true );
        while ( this.getActiveStatus() ) {
            byte[] msg = null;
            try {
                msg = this.sendQueue.poll( 1, TimeUnit.SECONDS );
            } catch ( InterruptedException ie ) {
                System.err.println( "ChunkServerConnection run InterruptedException: " + ie );
            }
            if ( msg == null ) {
                continue;
            } else {
                try {
                    this.sendData( msg ); // send the message along
                } catch ( Exception e ) {
                    System.err.println( "ChunkServerConnection run Exception: " + e );
                }
            }
        }
    }
}