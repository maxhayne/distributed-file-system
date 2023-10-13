package cs555.overlay.wireformats;

import java.io.*;

public class ControllerSendsStorageList implements Event {

	public byte type;
	public String filename;
	public String[] replicationServers;
	public String[] shardServers;

	public ControllerSendsStorageList(String filename, String[] replicationServers, 
			String[] shardServers) {
		this.type = Protocol.CONTROLLER_SENDS_STORAGE_LIST;
		this.filename = filename;
		if (replicationServers.length == 1 && replicationServers[0].isEmpty() ) {
			replicationServers = null;
		} else {
			this.replicationServers = replicationServers;
		}
		if (shardServers.length == 1 && shardServers[0].isEmpty() ) {
			this.shardServers = null;
		} else {
			this.shardServers = shardServers;
		}
	}

	public ControllerSendsStorageList( byte[] marshalledBytes ) throws IOException {
		ByteArrayInputStream bin = new ByteArrayInputStream( marshalledBytes );
        DataInputStream din = new DataInputStream( bin );

		type = din.readByte();

		int len = din.readInt();
		byte[] array = new byte[len];
		din.readFully( array );
		filename = new String( array );

		int replicationCount = din.readInt();
		if ( replicationCount != 0 ) {
			replicationServers = new String[replicationCount];
			for ( int i = 0; i < replicationCount; ++i ) {
				len = din.readInt();
				array = new byte[len];
				din.readFully( array );
				replicationServers[i] = new String( array );
			}
		}

		int shardCount = din.readInt();
		if ( shardCount != 0 ) {
			shardServers = new String[shardCount];
			for ( int i = 0; i < shardCount; ++i ) {
				len = din.readInt();
				array = new byte[len];
				din.readFully( array );
				shardServers[i] = new String( array );
			}
		}

		din.close();
		bin.close();
	}

	@Override
	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream( bout );

		dout.write( type );

		byte[] array = filename.getBytes();
		dout.writeInt( array.length );
		dout.write( array );

		// Write list of replication server address:port
		int replicationServerLength = 0;
		if ( replicationServers == null )
			dout.writeInt( 0 );
		else {
			replicationServerLength = replicationServers.length;
			dout.writeInt(replicationServers.length);
		}
		for ( int i = 0; i < replicationServerLength; ++i ) {
			array = replicationServers[i].getBytes();
			dout.writeInt( array.length );
			dout.write( array );
		}

		// Write list of shard server address:port
		int shardServerLength = 0;
		if ( shardServers == null )
			dout.writeInt(0);
		else {
			shardServerLength = shardServers.length;
			dout.writeInt( shardServers.length );
		}
		for ( int i = 0; i < shardServerLength; ++i ) {
			array = shardServers[i].getBytes();
			dout.writeInt( array.length );
			dout.write( array );
		}

		byte[] returnable = bout.toByteArray();
        dout.close();
        bout.close();
        return returnable;
	}

	@Override
	public byte getType() {
		return type;
	}
}
