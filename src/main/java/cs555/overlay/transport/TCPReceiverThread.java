package cs555.overlay.transport;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.node.ChunkServer;
import cs555.overlay.wireformats.*;
import cs555.overlay.util.Chunk;
import java.security.NoSuchAlgorithmException;
import java.net.UnknownHostException;
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.net.SocketException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Vector;
import java.net.Socket;

public class TCPReceiverThread extends Thread {

	private Socket socket;
	private TCPServerThread server;
	private DataInputStream din;
	private DataOutputStream dout;
	private boolean controller;

	// Will only be used at the Controller
	private ChunkServerConnectionCache chunkcache = null;
	// Will only be used if the computer on the other side of this connection registers
	private ChunkServerConnection chunkconn = null;

	// Will only be used at a Chunk Server for the connection to the Controller
	private ControllerConnection controllerconn = null;
	// Will only be used at a Chunk Server for file forwarding and functions for writing files
	private FileDistributionService fileservice = null;

	// Constructor for Chunk Server handling requests
	public TCPReceiverThread(Socket socket, TCPServerThread server, FileDistributionService fileservice) throws IOException {
		this.socket = socket;
		this.server = server;
		this.din = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 8192));
		this.dout = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 8192));
		this.controller = false;
		this.fileservice = fileservice;
	}

	// Regular constructor for at the Controller 
	public TCPReceiverThread(Socket socket, TCPServerThread server, ChunkServerConnectionCache chunkcache) throws IOException {
		this.socket = socket;
		this.server = server;
		this.din = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 8192));
		this.dout = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 8192));
		this.chunkcache = chunkcache;
		this.controller = true;
	}

	// Constructor for the TCPReceiverThread that will recieve messages over the registration
	// and heartbeat channel. Will not be spawned from a TCPServerThread
	public TCPReceiverThread(Socket socket, TCPServerThread server, ControllerConnection conn, FileDistributionService fileservice) throws IOException {
		this.socket = socket;
		this.server = server;
		this.din = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 8192));
		this.dout = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 8192));
		this.controller = false;
		this.controllerconn = conn;
		this.fileservice = fileservice;
	}

	public String getLocalAddress() throws UnknownHostException {
		return socket.getLocalAddress().toString().substring(1).split("\\:")[0];
	}

	public String getRemoteAddress() {
		return socket.getInetAddress().getHostAddress();
	}

	public int getLocalPort() {
		return socket.getLocalPort();
	}

	public int getRemotePort() {
		return socket.getPort();
	}

	public OutputStream getDataOutputStream() throws IOException {
		return socket.getOutputStream();
	}

	public synchronized void close() {
		try {
			if (socket != null) socket.close();
		} catch (IOException ioe) {}
		try {
			if (din != null) din.close();
			if (dout != null) dout.close();
		} catch (IOException ioe) {}	
		if (controller && chunkconn != null && chunkcache != null) {
			ChunkServerConnection tempConn = chunkconn;
			ChunkServerConnectionCache tempCache = chunkcache;
			chunkconn = null;
			chunkcache = null;
			tempCache.deregister(tempConn.getIdentifier());
		}
		if (controllerconn != null) controllerconn.setActiveStatus(false);
	}

	// Send a message back over this socket connection to the sender
	public boolean respond(byte[] msg) {
		try {
			int msgLength = msg.length;
			dout.writeInt(msgLength);
			dout.write(msg,0,msgLength);
			dout.flush();
			return true;
		} catch (IOException ioe) {
			return false;
		}
	}

	@Override
	public void run() {
		while (!this.socket.isClosed()) {
			try {
				
				int dataLength = din.readInt();
				byte[] data = new byte[dataLength];
				din.readFully(data, 0, dataLength);
				
				switch(data[0]) {
					case Protocol.CHUNK_SERVER_SENDS_REGISTRATION: {
						if (!controller) break;
						ChunkServerSendsRegistration msg = new ChunkServerSendsRegistration(data);
						ChunkServerConnection newconn = null;
						String info = "";
						int status;
						if (!msg.serveraddress.equals(this.getRemoteAddress())) { // Is not who they say they are
							info = "Registration request unsuccessful. Node " + this.getRemoteAddress() + " tried to register as " + msg.serveraddress + ".";
						} else if (this.chunkcache.isRegistered(msg.serveraddress, msg.serverport)) { // Is already in the connections cache
							info = "Registration request unsuccessful. Node [" + msg.serveraddress + ", " + msg.serverport + "] is already registered, but tried to register again.";
						} else if (this.chunkconn != null) {
							info = "Registration request unsuccessful. This socket has already registered as a chunk server.";
						} else { // Now must add the connection to TCPConnectionsCache
							newconn = chunkcache.register(this, msg.serveraddress, msg.serverport);
						}
						if (info == "" && newconn == null) {
							info = "Registration request unsuccessful. Cannot exceed 32 registered nodes.";
						} else if (newconn != null) {
							info = "Registration was successful. Your identifier is " + newconn.getIdentifier() + ".";
						}
						if (newconn == null) { // didn't register
							status = -1;
							ControllerReportsChunkServerRegistrationStatus response = new ControllerReportsChunkServerRegistrationStatus(status,info);
							respond(response.getBytes());
						} else { // did register
							status = newconn.getIdentifier();
							ControllerReportsChunkServerRegistrationStatus response = new ControllerReportsChunkServerRegistrationStatus(status,info);
							newconn.addToSendQueue(response.getBytes());
							chunkconn = newconn;
							chunkconn.setStartTime(System.currentTimeMillis());
						}
						msg = null;
						info = null;
						break;
					}

					case Protocol.CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS: {
						if (controller || controllerconn == null) break;
						ControllerReportsChunkServerRegistrationStatus msg = new ControllerReportsChunkServerRegistrationStatus(data);
						System.out.println(msg.info);
						if (msg.status == -1) {
							// Perhaps we should shutdown this tcpreceiver.
							if (this.controllerconn != null)
								this.controllerconn.shutdown();
							this.close();
							break;
						}
						// Need to start the heartbeat service in this ControllerConnection
						this.controllerconn.setIdentifier(msg.status);
						this.controllerconn.startHeartbeatService();
						msg = null;
						break;
					}

					case Protocol.CHUNK_SERVER_SENDS_DEREGISTRATION: {
						if (!controller || chunkcache == null) break;
						ChunkServerSendsDeregistration msg = new ChunkServerSendsDeregistration(data);
						chunkcache.deregister(msg.identifier);
						break;
					}

					case Protocol.CLIENT_REQUESTS_STORE_CHUNK: {
						if (!controller || chunkcache == null) break;
						ClientRequestsStoreChunk msg = new ClientRequestsStoreChunk(data);
						String servers = chunkcache.availableChunkServers(msg.filename,msg.sequence);
						if (servers.equals("")) {
							ControllerDeniesStorageRequest response = new ControllerDeniesStorageRequest();
							respond(response.getBytes());
							break;
						}
						String[] serverarray = servers.split(",");
						ControllerSendsClientValidChunkServers response = new ControllerSendsClientValidChunkServers(msg.filename,msg.sequence,serverarray);
						respond(response.getBytes());
						break;
					}

					case Protocol.CLIENT_REQUESTS_STORE_SHARDS: {
						if (!controller || chunkcache == null) break;
						ClientRequestsStoreShards msg = new ClientRequestsStoreShards(data);
						String servers = chunkcache.availableShardServers(msg.filename,msg.sequence);
						if (servers.equals("")) {
							ControllerDeniesStorageRequest response = new ControllerDeniesStorageRequest();
							respond(response.getBytes());
							break;
						}
						String[] serverarray = servers.split(",");
						ControllerSendsClientValidChunkServers response = new ControllerSendsClientValidChunkServers(msg.filename,msg.sequence,serverarray);
						respond(response.getBytes());
						break;
					}

					case Protocol.CLIENT_REQUESTS_FILE_DELETE: {
						if (!controller || chunkcache == null) break;
						ClientRequestsFileDelete msg = new ClientRequestsFileDelete(data);
						ControllerApprovesFileDelete response = new ControllerApprovesFileDelete();
						respond(response.getBytes()); // Approve it
						// Now try to send a message out to all servers in the connectioncache to delete the file
						String temp = chunkcache.getAllServerAddresses();
						if (temp.equals("")) break;
						ControllerRequestsFileDelete deleteMessage = new ControllerRequestsFileDelete(msg.filename);
						String[] addresses = temp.split(",");
						for (int i = 0; i < addresses.length; i++) {
							String address = addresses[i].split(":")[0];
							int port = Integer.valueOf(addresses[i].split(":")[1]);
							try {
								Socket deleteAttempt = new Socket(address,port);
								deleteAttempt.setSoTimeout(2000);
								TCPSender sender = new TCPSender(deleteAttempt);
								sender.sendData(deleteMessage.getBytes());
								byte[] reply = sender.receiveData();
								if (reply[0] == Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE)	{
									ChunkServerAcknowledgesFileDelete msgreply = new ChunkServerAcknowledgesFileDelete(reply);
									//System.out.println("Chunk Server " + addresses[i] + " acknowledges it should delete " + msgreply.filename + ".");
								} else {
									//System.out.println("No delete acknowledgement was received for file " + msg.filename + " during the one second interval.");
								}
								deleteAttempt.close();
							} catch (Exception e) {
								// There was a problem, but this was best effort.
							}
						}
						// Now remove from recommendations
						chunkcache.removeFileFromRecommendations(msg.filename);
						break;
					}

					case Protocol.CONTROLLER_REQUESTS_FILE_DELETE: {
						if (controller || fileservice == null) break;
						ControllerRequestsFileDelete msg = new ControllerRequestsFileDelete(data);
						fileservice.deleteFile(msg.filename);
						ChunkServerAcknowledgesFileDelete response = new ChunkServerAcknowledgesFileDelete(msg.filename);
						respond(response.getBytes());
						break;
					}

					case Protocol.SENDS_FILE_FOR_STORAGE: {
						if (controller || fileservice == null) break;
						SendsFileForStorage msg = new SendsFileForStorage(data);
						ChunkServerAcknowledgesFileForStorage response = new ChunkServerAcknowledgesFileForStorage(msg.filename);
						//System.out.println("storing: " + msg.filename);
						respond(response.getBytes()); // Send acknowledgement of receive
						//System.out.println("Received storage request.");
						// Prepare the byte[] for being storage, whether it is a chunk or a shard.
						// Need to save the file if possible, then open a new socket to connect to the next node in the chain.
						// Then, send out the forward message with the node we are sending to's forwarding information removed.
						byte[] prepared;
						if (FileDistributionService.checkChunkFilename(msg.filename)) { // Is a chunk
							int sequence = Integer.valueOf(msg.filename.split("_chunk")[1]);
							int version = 0;
							try {
								prepared = fileservice.readyChunkForStorage(sequence,version,msg.filedata);
							} catch (NoSuchAlgorithmException nsae) {
								prepared = null;
							}
						} else if (FileDistributionService.checkShardFilename(msg.filename)) { // Is a shard
							String[] parts = msg.filename.split("_chunk");
							int sequence = Integer.valueOf(parts[1].split("_shard")[0]);
							int shardnumber = Integer.valueOf(parts[1].split("_shard")[1]);
							int version = 0;
							prepared = fileservice.readyShardForStorage(sequence,shardnumber,version,msg.filedata);
						} else {
							break;
						}
						boolean saved;
						if (prepared != null) {
							saved = fileservice.overwriteNewFile(fileservice.getDirectory()+msg.filename,prepared);
						} else {
							saved = false;
						}
						Vector<String> unreachableServers = new Vector<String>();
						if (msg.servers != null) {
							for (int i = 0; i < msg.servers.length; i++) {
								String address = msg.servers[i].split(":")[0];
								int port = Integer.valueOf(msg.servers[i].split(":")[1]);
								try {
									//System.out.println("Trying to reach the server " + i + ".");
									Socket storeAttempt = new Socket(address,port);
									storeAttempt.setSoTimeout(2000);
									TCPSender sender = new TCPSender(storeAttempt);
									String[] forwardServers;
									if (i+1 < msg.servers.length)
										forwardServers = Arrays.copyOfRange(msg.servers, i+1, msg.servers.length);
									else
										forwardServers = null;
									SendsFileForStorage forwardMessage = new SendsFileForStorage(msg.filename,msg.filedata,forwardServers);
									sender.sendData(forwardMessage.getBytes());
									byte[] reply = sender.receiveData();
									if (reply[0] == Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_FOR_STORAGE)	{
										ChunkServerAcknowledgesFileForStorage msgreply = new ChunkServerAcknowledgesFileForStorage(reply);
										//System.out.println("Chunk Server " + msg.servers[i] + " acknowledges it should store " + msgreply.filename + ".");
										storeAttempt.close();
										break; // break out of loop
									} else {
										//System.out.println("No store acknowledgement was received for file " + msg.filename + " during the one second interval.");
										unreachableServers.add(msg.servers[i]);
										storeAttempt.close();
									}
								} catch (Exception e) {
									//System.out.println(e);
									//e.printStackTrace();
									unreachableServers.add(msg.servers[i]);
								}
							}
						}
						if (!saved) unreachableServers.add(getLocalAddress() + String.valueOf(this.server.getLocalPort()));
						if (unreachableServers.size() > 0) {
							try {
								Socket controllerSocket = new Socket(ChunkServer.CONTROLLER_HOSTNAME,ChunkServer.CONTROLLER_PORT);
								TCPSender controllerSender = new TCPSender(controllerSocket);
								for (String server : unreachableServers) {
									ChunkServerNoStoreFile tellController = new ChunkServerNoStoreFile(server,msg.filename);
									controllerSender.sendData(tellController.getBytes());
								}
								controllerSocket.close();
							} catch (Exception e) {
								// This is best effort.
							}
						}
						break;
					}

					case Protocol.REQUESTS_CHUNK: {
						if (controller) break;
						RequestsChunk msg = new RequestsChunk(data);
						// Deny if not in the format of a chunk
						if (!FileDistributionService.checkChunkFilename(msg.filename)) {
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							break;
						}
						// Try to read the data from a chunk and send it back over the socket.
						String filename = fileservice.getDirectory() + msg.filename;
						byte[] filedata = fileservice.readBytesFromFile(filename);
						// Deny if for some reason nothing was read
						if (filedata == null) {
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							// If the file is gone, the Controller should figure it out on the next major heartbeat
							respond(response.getBytes());
							break;
						}
						Vector<Integer> errors;
						try {
							errors = fileservice.checkChunkForCorruption(filedata);
						} catch (NoSuchAlgorithmException nsae) {
							System.out.println("TCPReceiverThread at REQUESTS_CHUNK: Can't use SHA1.");
							errors = new Vector<Integer>();
						}
						if (errors.size() != 0) { // Need to report the file corruption and deny the request.
							int[] slices = new int[errors.size()];
							for (int i = 0; i < errors.size(); i++) 
								slices[i] = errors.elementAt(i);
							ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(fileservice.getIdentifier(),msg.filename,slices);
							fileservice.addToQueue(event);
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							break;
						}
						// There were no errors
						byte[] justdata = fileservice.getDataFromChunk(fileservice.removeHashesFromChunk(filedata));
						ChunkServerServesFile response = new ChunkServerServesFile(msg.filename,justdata);
						respond(response.getBytes());
						break;
					}

					case Protocol.REQUESTS_SHARD: {
						if (controller) break;
						RequestsShard msg = new RequestsShard(data);
						// Deny if not in the format of a shard
						if (!FileDistributionService.checkShardFilename(msg.filename)) {
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							break;
						}
						// Try to read the data from a chunk and send it back over the socket.
						String filename = fileservice.getDirectory() + msg.filename;
						byte[] filedata = fileservice.readBytesFromFile(filename);
						// Deny if for some reason nothing was read
						if (filedata == null) {
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							// If the file is gone, the Controller should figure it out on the next major heartbeat
							break;
						}
						// Start new code here
						boolean corrupt;
						try {
							corrupt = fileservice.checkShardForCorruption(filedata);
						} catch (NoSuchAlgorithmException nsae) {
							System.out.println("TCPReceiverThread at REQUESTS_SHARD: Can't use SHA1.");
							corrupt = false;
						}
						if (corrupt) { // Need to report the file corruption and deny the request.
							ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(fileservice.getIdentifier(),msg.filename,null);
							fileservice.addToQueue(event);
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							break;
						}
						// There were no errors
						byte[] justdata = fileservice.getDataFromShard(fileservice.removeHashFromShard(filedata));
						ChunkServerServesFile response = new ChunkServerServesFile(msg.filename,justdata);
						respond(response.getBytes());
						break;
					}

					case Protocol.CHUNK_SERVER_SENDS_HEARTBEAT: {
						if (!controller || chunkconn == null) break;
						ChunkServerSendsHeartbeat msg = new ChunkServerSendsHeartbeat(data);
						// Read the type, read totalchunks, read freespace, leave files unread.
						// the HeartbeatMonitor can do that for us.
						chunkconn.updateFreeSpaceAndChunks(msg.freespace,msg.totalchunks);
						byte[] files = Arrays.copyOfRange(data,17,data.length);
						chunkconn.updateHeartbeatInfo(System.currentTimeMillis(),msg.type,files);
						msg = null;
						files = null;
						break;
					}

					case Protocol.CONTROLLER_SENDS_HEARTBEAT: {
						if (controller || controllerconn == null) break;
						ChunkServerRespondsToHeartbeat response = new ChunkServerRespondsToHeartbeat();
						respond(response.getBytes());
						break;
					}

					case Protocol.CHUNK_SERVER_RESPONDS_TO_HEARTBEAT: {
						if (!controller || chunkconn == null) break;
						chunkconn.incrementPokeReplies();
						break;
					}
					
					case Protocol.CHUNK_SERVER_REPORTS_FILE_CORRUPTION: {
						if (!controller || chunkcache == null) break;
						ChunkServerReportsFileCorruption msg = new ChunkServerReportsFileCorruption(data);
						String filename;
						int sequence;
						if (FileDistributionService.checkChunkFilename(msg.filename)) { // This is a Chunk
							String[] parts = msg.filename.split("_chunk");
							filename = parts[0];
							sequence = Integer.parseInt(parts[1]);
							chunkcache.markChunkCorrupt(filename,sequence,msg.identifier); // Mark the chunk corrupt
						} else { // This is a shard
							String[] parts = msg.filename.split("_chunk");
							filename  = parts[0];
							String[] parts2 = parts[1].split("_shard");
							sequence = Integer.parseInt(parts2[0]);
							int shardnumber = Integer.parseInt(parts2[1]);
							chunkcache.markShardCorrupt(filename,sequence,shardnumber,msg.identifier);
						}
						String info = chunkcache.getChunkStorageInfo(filename,sequence);
						if (info.equals("|")) { // Nothing we can do
							msg = null;
							filename = null;
							info = null;
							break;
						}
						String[] parts = info.split("\\|",-1);
						String[] replications = parts[0].split(",");
						String[] shards = parts[1].split(",");
						ControllerSendsStorageList response = new ControllerSendsStorageList(filename,replications,shards);
						respond(response.getBytes());
						parts = null;
						response = null;
						msg = null;
						filename = null;
						info = null;
						replications = null;
						shards = null;
						break;
					}

					case Protocol.CONTROLLER_REQUESTS_FILE_ACQUIRE: {
						if (controller || fileservice == null) break;
						Event event = EventFactory.getEvent(data[0],data);
						this.fileservice.addToQueue(event); // Add to fileservice queue
						ControllerRequestsFileAcquire cast = (ControllerRequestsFileAcquire)event;
						ChunkServerAcknowledgesFileAcquire response = new ChunkServerAcknowledgesFileAcquire(cast.filename);
						respond(response.getBytes());
						break;
					}

					case Protocol.CHUNK_SERVER_NO_STORE_FILE:  {
						if (!controller || chunkcache == null) break;
						ChunkServerNoStoreFile msg = new ChunkServerNoStoreFile(data);
						// We need to remove the Chunk with those properties from the recommendations, and find a new server that can store the file.
						// First, find the identifier of the Chunk Server that has that address:
						String address = msg.address.split(":")[0];
						int port = Integer.valueOf(msg.address.split(":")[1]);
						int identifier = chunkcache.getChunkServerIdentifier(address,port);
						int sequence = Integer.valueOf(msg.filename.split("_chunk")[1]);
						// Just remove the file for now. Can try to repair the file system during heartbeats for chunks that aren't replicated 3 times.
						chunkcache.removeChunkFromRecommendations(new Chunk(msg.filename,sequence,0,identifier,false));
						break;
					}

					case Protocol.REQUESTS_SLICES: {
						if (controller) break;
						RequestsSlices msg = new RequestsSlices(data);
						// Deny if not in the format of a chunk
						if (!FileDistributionService.checkChunkFilename(msg.filename)) {
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							break;
						}
						// Try to read the data from a chunk and send it back over the socket.
						String filename = fileservice.getDirectory() + msg.filename;
						byte[] filedata = fileservice.readBytesFromFile(filename);
						// Deny if for some reason nothing was read
						if (filedata == null) {
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							break;
						}
						Vector<Integer> errors;
						try {
							errors = fileservice.checkChunkForCorruption(filedata);
						} catch (NoSuchAlgorithmException nsae) {
							System.out.println("TCPReceiverThread at REQUESTS_CHUNK: Can't use SHA1.");
							errors = new Vector<Integer>();
						}
						Vector<Integer> healthy = new Vector<Integer>();
						for (int i = 0; i < msg.slices.length; i++) { // If our version of the slice is healthy, add to healthy vector
							if (!errors.contains(msg.slices[i])) {
								healthy.add(msg.slices[i]);
							}
						}
						if (healthy.size() == 0) { // If we have no healthy slices to serve, deny request and add Corruption event to fileservice
							ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
							respond(response.getBytes());
							int[] slices = new int[errors.size()];
							for (int i = 0; i < errors.size(); i++) 
								slices[i] = errors.elementAt(i);
							ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(fileservice.getIdentifier(),filename,slices);
							fileservice.addToQueue(event);
						} else {
							int[] cleanslices = new int[healthy.size()];
							for (int i = 0; i < healthy.size(); i++) 
								cleanslices[i] = healthy.elementAt(i);
							// Created slices array we can serve
							byte[][] slicedata = fileservice.getSlices(filedata,cleanslices);
							ChunkServerServesSlices serveSlices = new ChunkServerServesSlices(msg.filename,cleanslices,slicedata);
							respond(serveSlices.getBytes()); // Serve the slices
							if (errors.size() != 0) {
								int[] slices = new int[errors.size()];
								for (int i = 0; i < errors.size(); i++) 
									slices[i] = errors.elementAt(i);
								ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(fileservice.getIdentifier(),filename,slices);
								fileservice.addToQueue(event);
							}
						}
						break;
					}

					case Protocol.CHUNK_SERVER_REPORTS_FILE_FIX: {
						if (!controller || chunkcache == null) break;
						ChunkServerReportsFileFix msg = new ChunkServerReportsFileFix(data);
						String filename;
						int sequence;
						if (FileDistributionService.checkChunkFilename(msg.filename)) { // This is a Chunk
							String[] parts = msg.filename.split("_chunk");
							filename = parts[0];
							sequence = Integer.parseInt(parts[1]);
							chunkcache.markChunkHealthy(filename,sequence,msg.identifier); // Mark the chunk corrupt
						} else { // This is a shard
							String[] parts = msg.filename.split("_chunk");
							filename  = parts[0];
							String[] parts2 = parts[1].split("_shard");
							sequence = Integer.parseInt(parts2[0]);
							int shardnumber = Integer.parseInt(parts2[1]);
							chunkcache.markShardHealthy(filename,sequence,shardnumber,msg.identifier);
						}
						// No need to respond
						break;
					}

					case Protocol.CLIENT_REQUESTS_FILE_SIZE: {
						if (!controller || chunkcache == null) break;
						ClientRequestsFileSize msg = new ClientRequestsFileSize(data);
						ControllerReportsFileSize response = new ControllerReportsFileSize(msg.filename,chunkcache.getFileSize(msg.filename));
						respond(response.getBytes());
						break;
					}

					case Protocol.CLIENT_REQUESTS_FILE_STORAGE_INFO: {
						if (!controller || chunkcache == null) break;
						ClientRequestsFileStorageInfo msg = new ClientRequestsFileStorageInfo(data);
						String filename;
						int sequence;
						//System.out.println(msg.filename + " " + FileDistributionService.checkChunkFilename(msg.filename));
						if (FileDistributionService.checkChunkFilename(msg.filename)) { // This is a Chunk
							String[] parts = msg.filename.split("_chunk");
							filename = parts[0];
							sequence = Integer.parseInt(parts[1]);
						} else { // This is a shard
							String[] parts = msg.filename.split("_chunk");
							filename  = parts[0];
							String[] parts2 = parts[1].split("_shard");
							sequence = Integer.parseInt(parts2[0]);
						}
						String info = chunkcache.getChunkStorageInfo(filename,sequence);
						//System.out.println("INFO FOR CHUNK " + sequence + " OF " + filename + ": " + info);
						if (info.equals("|")) { // Nothing we can do
							msg = null;
							filename = null;
							info = null;
							break;
						}
						String[] parts = info.split("\\|",-1);
						String[] replications = parts[0].split(",");
						String[] shards = parts[1].split(",");
						ControllerSendsStorageList response = new ControllerSendsStorageList(filename,replications,shards);
						respond(response.getBytes());
						break;
					}

					case Protocol.CLIENT_REQUESTS_FILE_LIST: {
						if (!controller || chunkcache == null) break;
						String[] list = chunkcache.getFileList();
						ControllerSendsFileList listReply = new ControllerSendsFileList(list);
						respond(listReply.getBytes());
						break;
					}

					// Data is sent out including the hashes and metadata.
					case Protocol.CHUNK_SERVER_REQUESTS_FILE: {
						if (controller || fileservice == null) break;
						ChunkServerRequestsFile msg = new ChunkServerRequestsFile(data);
						byte[] filedata = fileservice.readBytesFromFile(fileservice.getDirectory()+msg.filename);
						if (filedata == null) {
							ChunkServerDeniesRequest deny = new ChunkServerDeniesRequest(msg.filename);
							respond(deny.getBytes());
							break;
						}
						if (FileDistributionService.checkChunkFilename(msg.filename)) { // It's a Chunk
							Vector<Integer> errors;
							try {
								errors = FileDistributionService.checkChunkForCorruption(filedata);
							} catch (NoSuchAlgorithmException nsae) {
								System.out.println(nsae);
								break;
							}
							if (errors.size() != 0) {
								int[] slices = new int[errors.size()];
								for (int i = 0; i < errors.size(); i++) 
									slices[i] = errors.elementAt(i);
								ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(fileservice.getIdentifier(),msg.filename,slices);
								fileservice.addToQueue(event);
								ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
								respond(response.getBytes());
								break;
							}
							byte[] sendData;
							if (filedata.length != 65720) {
								sendData = Arrays.copyOfRange(filedata,0,65720);
							} else {
								sendData = filedata;
							}
							ChunkServerServesFile serve = new ChunkServerServesFile(msg.filename,sendData);
							respond(serve.getBytes());
						}  else { // It's a Shard
							boolean corrupt;
							try {
								corrupt = FileDistributionService.checkShardForCorruption(filedata);
							} catch (NoSuchAlgorithmException nsae) {
								System.out.println(nsae);
								break;
							}
							if (corrupt) {
								ChunkServerReportsFileCorruption event = new ChunkServerReportsFileCorruption(fileservice.getIdentifier(),msg.filename,null);
								fileservice.addToQueue(event);
								ChunkServerDeniesRequest response = new ChunkServerDeniesRequest(msg.filename);
								respond(response.getBytes());
								break;
							}
							byte[] sendData = FileDistributionService.getDataFromShard(FileDistributionService.removeHashFromShard(filedata));
							ChunkServerServesFile serve = new ChunkServerServesFile(msg.filename,sendData);
							respond(serve.getBytes());
						}
						break;
					}

					case Protocol.CHUNK_SERVER_ACKNOWLEDGES_FILE_ACQUIRE: {
						break;
					}

					default : {
						System.out.println("Unknown message received.");
						break;
					}
				}
			} catch (SocketException se) {
				//System.err.println("TCPReceiverThread run SocketException: " + se);
				break;
			} catch (IOException ioe) {
				//System.err.println("TCPReceiverThread run IOException: " + ioe);
				break;
			}
		}
		this.close(); // Try to close the receiver if the while loop has exited.
	}
}