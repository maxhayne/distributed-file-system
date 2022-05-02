package cs555.overlay.node;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.transport.TCPSender;
import cs555.overlay.wireformats.*;
import cs555.overlay.node.ChunkServer;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.Collection;
import java.io.IOException;
import java.util.Arrays;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.Scanner;
import java.net.Socket;
import java.io.File;
import java.util.Map;
import java.util.HashMap;

public class Client {

	public static final int DATA_SHARDS = 6; 
	public static final int PARITY_SHARDS = 3; 
	public static final int TOTAL_SHARDS = 9;
	public static final int BYTES_IN_INT = 4;
	public static final int BYTES_IN_LONG = 8;
	public static final String CONTROLLER_HOSTNAME = "192.168.68.59";
	public static final int CONTROLLER_PORT = 50000;


	private static byte[] getShardFromServer(String filename, String address, Map<String,TCPSender> tcpConnections) {
		TCPSender connection = getTCPSender(tcpConnections,address);
		if (connection == null) {
			System.out.println("Couldn't esablish a connection.");
			return null;
		}
		try {
			RequestsShard request = new RequestsShard(filename);
			connection.sendData(request.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null) {
				//System.out.println("Null Reply");
				return null;
			} else if (reply[0] == Protocol.CHUNK_SERVER_DENIES_REQUEST) {
				//System.out.println("CHUNK_SERVER_DENIES_REQUEST");
				return null;
			}
			ChunkServerServesFile serve = new ChunkServerServesFile(reply);
			//System.out.println(serve.filedata.length);
			return serve.filedata;
		} catch (Exception e) { return null; }
	}

	private static byte[] getChunkFromServer(String filename, String address, Map<String,TCPSender> tcpConnections) {
		TCPSender connection = getTCPSender(tcpConnections,address);
		if (connection == null) {
			System.out.println("Couldn't esablish a connection.");
			return null;
		}
		try {
			RequestsChunk request = new RequestsChunk(filename);
			connection.sendData(request.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null || reply[0] == Protocol.CHUNK_SERVER_DENIES_REQUEST) return null;
			ChunkServerServesFile serve = new ChunkServerServesFile(reply);
			return serve.filedata;
		} catch (Exception e) { return null; }
	}

	private static byte[] getChunkFromReplicationServers(String filename, String[] servers, Map<String,TCPSender> tcpConnections) {
		if (servers == null) return null;
		for (String server : servers) {
			//System.out.println(server);
			byte[] data = getChunkFromServer(filename,server,tcpConnections);
			if (data != null) return data;
		}
		return null;
	}

	// filename will be filename_chunk#, so must append "_shard#"
	private static byte[][] getShardsFromServers(String filename, String[] servers, Map<String,TCPSender> tcpConnections) {
		//for (String server : servers) System.out.println(server);
		if (servers == null) return null;
		byte[][] shards = new byte[ChunkServer.TOTAL_SHARDS][];
		int index = -1;
		for (String server : servers) {
			index++;
			if (server.equals("-1")) continue;
			String[] parts = server.split(":");
			String shardname = filename + "_shard" + String.valueOf(index);
			byte[] filedata = getShardFromServer(shardname,server,tcpConnections);
			if (filedata == null) continue;
			shards[index] = filedata;
		}
		return FileDistributionService.getShardsFromShards(shards);
	}

	private static Socket getSocket(String hostname, int port, int timeout) {
		try {
			Socket socket = new Socket(hostname,port);
			socket.setSoTimeout(timeout);
			socket.setKeepAlive(true);
			return socket;
		} catch(ConnectException ce) {
			System.out.println("Failed to connect: " + ce);
			return null;
		} catch(UnknownHostException uhe) {
			System.out.println("Failed to connect: " + uhe);
			return null;
		} catch(IOException ioe) {
			System.out.println("The connection was terminated: " + ioe);
			ioe.printStackTrace();
			return null;
		}
	}

	public static TCPSender getTCPSender(Map<String,TCPSender> tcpConnections, String address) {
		if (tcpConnections.containsKey(address))
			return tcpConnections.get(address);
		try {
			String hostname = address.split(":")[0];
			int port = Integer.valueOf(address.split(":")[1]);
			Socket socket = new Socket(hostname,port);
			socket.setSoTimeout(3000);
			TCPSender newConnection = new TCPSender(socket);
			tcpConnections.put(address,newConnection);
			return newConnection;
		} catch(ConnectException ce) {
			System.out.println("Failed to connect: " + ce);
			return null;
		} catch(UnknownHostException uhe) {
			System.out.println("Failed to connect: " + uhe);
			return null;
		} catch(IOException ioe) {
			System.out.println("The connection was terminated: " + ioe);
			ioe.printStackTrace();
			return null;
		}
	}

	private static String listfiles(Map<String,TCPSender> tcpConnections) {
		String returnable = "";
		String address = CONTROLLER_HOSTNAME + ":" + String.valueOf(CONTROLLER_PORT);
		TCPSender connection = getTCPSender(tcpConnections,address);
		if (connection == null) {
			System.out.println("Couldn't esablish a connection with the Controller.");
			return "";
		}
		try {
			ClientRequestsFileList listRequest = new ClientRequestsFileList();
			connection.sendData(listRequest.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null) {
				System.out.println("The Controller didn't respond for a request of files.");
				return "";
			}
			ControllerSendsFileList list = new ControllerSendsFileList(reply);
			if (list.list == null) {
				return "";
			}
			for (String file : list.list) {
				returnable += file + "\n";
			}
			return returnable;
		} catch (IOException ioe) {
			System.out.println("There was a problem receiving the file list.");
			return "";
		}
	}

	private static void store(int schema, String filename, Map<String,TCPSender> tcpConnections) {
		Path path = Paths.get(filename);
		Path name = path.getFileName();
		String basename = name.toString();
		String address = CONTROLLER_HOSTNAME + ":" + String.valueOf(CONTROLLER_PORT);
		TCPSender connection = getTCPSender(tcpConnections,address);
		if (connection == null) {
			System.out.println("Couldn't establish a connection with the Controller.");
			return;
		}
		try {
			int index = 0;
			boolean finished = false;
			while (!finished) {
				byte[] newchunk = FileDistributionService.getNextChunkFromFile(filename,index);
				if (newchunk == null) {
					finished = true;
					break;
				}
				if (!finished) {
					boolean sentToServers = false;
					if (schema == 0) { // We are replicating
						ClientRequestsStoreChunk request = new ClientRequestsStoreChunk(basename,index);
						connection.sendData(request.getBytes());
						byte[] data = connection.receiveData();
						if (data == null) {
							System.out.println("No message received from Controller for chunk " + index);
							finished = false;
							break;
						} else if (data[0] == Protocol.CONTROLLER_DENIES_STORAGE_REQUEST) {
							System.out.println("The Controller denied the storage request of chunk " + index);
							finished = false;
							break;
						}
						ControllerSendsClientValidChunkServers response = new ControllerSendsClientValidChunkServers(data);
						// Now need to send the newchunk to the first available Chunk Server in the list.
						for (int i = 0; i < response.servers.length; i++) {
							TCPSender serverConnection = getTCPSender(tcpConnections,response.servers[i]);
							if (serverConnection == null) {
								continue;
							}
							try {
								String chunkFilename = basename + "_chunk" + String.valueOf(index);
								String[] forwardServers = new String[response.servers.length-1];
								int addIndex = 0;
								for (int j = 0; j < response.servers.length; j++) {
									if (j != i) {
										forwardServers[addIndex] = response.servers[j];
										addIndex++;
									}
								}
								SendsFileForStorage storeChunk = new SendsFileForStorage(chunkFilename,newchunk,forwardServers);
								serverConnection.sendData(storeChunk.getBytes());
								byte[] storeResponse = serverConnection.receiveData();
								if (storeResponse == null) { continue; }
								ChunkServerAcknowledgesFileForStorage acknowledge = new ChunkServerAcknowledgesFileForStorage(storeResponse);
								sentToServers = true;
								break;
							} catch (Exception e) {}
						}
					} else { // We are sharding
						ClientRequestsStoreShards request = new ClientRequestsStoreShards(basename,index);
						connection.sendData(request.getBytes());
						byte[] data = connection.receiveData();
						if (data == null) {
							System.out.println("No message received from Controller for chunk " + index);
							finished = false;
							break;
						} else if (data[0] == Protocol.CONTROLLER_DENIES_STORAGE_REQUEST) {
							System.out.println("The Controller denied the storage request of chunk " + index);
							finished = false;
							break;
						}
						ControllerSendsClientValidShardServers response = new ControllerSendsClientValidShardServers(data);
						// Need to create shards
						byte[] chunkForStorage;
						try {
							chunkForStorage = FileDistributionService.readyChunkForStorage(index,0,newchunk);
						} catch (Exception e) {
							System.out.println("store: SHA1 is not available.");
							break;
						}
						byte[][] shards = FileDistributionService.makeShardsFromChunk(chunkForStorage);
						for (int i = 0; i < response.servers.length; i++) {
							TCPSender serverConnection = getTCPSender(tcpConnections,response.servers[i]);
							if (serverConnection == null) {
								System.out.println("Couldn't establish a connection with " + response.servers[i] + ". Stopping.");
								break;
							}
							try {
								String shardFilename = basename + "_chunk" + String.valueOf(index) + "_shard" + String.valueOf(i);
								//System.out.println(shardFilename);
								SendsFileForStorage storeShard = new SendsFileForStorage(shardFilename,shards[i],null);
								serverConnection.sendData(storeShard.getBytes());
								byte[] storeResponse = serverConnection.receiveData();
								if (storeResponse == null) { // Try the next server
									System.out.println("Shard server didn't acknowledge storage request for '" + shardFilename + "', stopping the storage operation.");
									sentToServers = false;
									break;
								}
							} catch (Exception e) {
								sentToServers = false;
								break;
							}
							if (i == response.servers.length-1) sentToServers = true;
						}

					}
					if (sentToServers == false) break;
				}
				index++;
			}
			if (!finished) {
				// Request to delete the file from the controller
				ClientRequestsFileDelete delete = new ClientRequestsFileDelete(basename);
				connection.sendData(delete.getBytes());
				byte[] deleteResponse = connection.receiveData();
				if (deleteResponse == null) {
					System.out.println("The storage operation was unsuccessful. Controller didn't respond to a delete request.");
				} else if (deleteResponse[0] == Protocol.CONTROLLER_APPROVES_FILE_DELETE) {
					System.out.println("The storage operation was unsuccessful. Controller approved the deletion of the file.");
				}
				return;
			}
			System.out.println("The storage operation was successful.");
			connection = null;
		} catch (SocketTimeoutException ste) {
			System.out.println("Socket timed out: " + ste);
		} catch (SocketException se) {
			System.out.println("Socket exception: " + se);
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe);
		}
	}

	private static void delete(String filename, Map<String,TCPSender> tcpConnections) {
		Path path = Paths.get(filename);
		Path name = path.getFileName();
		String basename = name.toString();
		String address = CONTROLLER_HOSTNAME + ":" + String.valueOf(CONTROLLER_PORT);
		TCPSender connection = getTCPSender(tcpConnections,address);
		if (connection == null) {
			System.out.println("Couldn't esablish a connection with the Controller.");
			return;
		}
		try {
			ClientRequestsFileDelete delete = new ClientRequestsFileDelete(basename);
			connection.sendData(delete.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null) {
				System.out.println("The Controller didn't respond to the delete request for file '" + basename + "'");
			} else if (reply[0] == Protocol.CONTROLLER_APPROVES_FILE_DELETE) {
				System.out.println("The Controller has acknowledged the request to delete file '" + basename + "'");
			}
			connection = null;
		} catch (SocketTimeoutException ste) {
			System.out.println("Socket timed out: " + ste);
		} catch (SocketException se) {
			System.out.println("Socket exception: " + se);
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe);
		}
	}

	private static void retrieve(int schema, String filename, String location, Map<String,TCPSender> tcpConnections) {
		Path path = Paths.get(filename);
		Path name = path.getFileName();
		String basename = name.toString();
		if (!location.endsWith("/")) location += "/";
		File test = new File(location+basename);
		if (test.exists()) {
			System.out.println("'" + location + basename + "' already exists. This operation will append it.");
		}
		String address = CONTROLLER_HOSTNAME + ":" + String.valueOf(CONTROLLER_PORT);
		TCPSender connection = getTCPSender(tcpConnections,address);
		if (connection == null) {
			System.out.println("Couldn't esablish a connection with the Controller.");
			return;
		}
		byte[] reply = null;
		try {
			ClientRequestsFileSize size = new ClientRequestsFileSize(basename);
			connection.sendData(size.getBytes());
			reply = connection.receiveData();
			if (reply == null) {
				System.out.println("The Controller didn't respond to the size request for file '" + basename + "'");
				return;
			}
			ControllerReportsFileSize reportedsize = new ControllerReportsFileSize(reply); // read for total chunks
			boolean finished = false;
			int lastchunk = 0;
			System.out.println("total chunks: " + reportedsize.totalchunks);
			// Loop here for every chunk that needs retrieving
			for (int i = 0; i < reportedsize.totalchunks; i++) {
				String chunkName = basename + "_chunk" + String.valueOf(i);
				ClientRequestsFileStorageInfo infoRequest = new ClientRequestsFileStorageInfo(chunkName);
				connection.sendData(infoRequest.getBytes());
				reply = connection.receiveData();
				if (reply == null) {
					System.out.println("The Controller didn't respond to the info request for '" + chunkName + "'");
					return;
				}
				ControllerSendsStorageList storageList = new ControllerSendsStorageList(reply);
				// Check which schema we are using...
				byte[] download = null;
				if (schema == 0) {
					download = getChunkFromReplicationServers(chunkName,storageList.replicationservers,tcpConnections);
				} else {
					byte[][] shards = getShardsFromServers(chunkName,storageList.shardservers,tcpConnections);
					if (shards != null) {
						download = FileDistributionService.getChunkFromShards(shards);
						download = FileDistributionService.removeHashesFromChunk(download);
						download = FileDistributionService.getDataFromChunk(download);
					}
				}				
				if (download == null) {
					System.out.println("Couldn't retrieve '" + chunkName + "'. Stopping the download.");
					break;
				} else {
					// We have the data, now need to write the data to a file.
					FileDistributionService.appendFile(location+basename,download);
					lastchunk++;
					if (i == reportedsize.totalchunks-1) finished = true;
				}
			}
			// Supposedly we are done writing the file to disk.
			if (finished) {
				System.out.println("'" + basename + "' has been successfully saved to '" + location + "'.");
			} else {
				System.out.println("'" + basename + "' was downloaded until chunk number " + lastchunk + "'.");
				System.out.println("It is stored on disk in the location specified, though uncomplete.");
			}
		} catch (SocketTimeoutException ste) {
			System.out.println("Socket timed out: " + ste);
		} catch (SocketException se) {
			System.out.println("Socket exception: " + se);
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe);
		}
	}

	public static void main(String[] args){
		int storageSchema;
		// Only one optional command line argument -- type of storage to use: 'erasure' or 'replication'
		if (args.length > 0) {
			if (args[0].equalsIgnoreCase("replication")) {
				storageSchema = 0;
			} else if (args[0].equalsIgnoreCase("erasure")) {
				storageSchema = 1;
			} else {
				System.out.println("'" + args[0] + "' does not match 'replication' or 'erasure', defaulting to replication.");
				storageSchema = 0;
			}
		} else {
			System.out.println("No storage schema specified. Defaulting to replication.");
			storageSchema = 0;
		}

		Map<String,TCPSender> tcpConnections = new HashMap<String,TCPSender>();
		System.out.println("Use command 'help' to list available commands and usage.");

		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.print("COMMAND: ");
			String command = scanner.nextLine();
			String[] parts = command.split("\\s+");
			if (parts.length > 3) {
				System.out.println("No command uses more than three arguments. Use 'help' for help.");
				continue;
			}
			if (parts[0].equalsIgnoreCase("help")) {
				System.out.println("'list' -- print a list all files stored in the file system.");
				System.out.println("'store' [path/FILENAME] -- store a file on the distributed file system.");
				System.out.println("'delete' [FILENAME] -- delete a file from the distributed file system.");
				System.out.println("'retrieve' [FILENAME] [path/DOWNLOAD_LOCATION] -- retrieve a file from the distributed file system and deposit it in a select location.");
				System.out.println("'exit' -- stop the program on the client's side.");
			} else if (parts[0].equalsIgnoreCase("list")) {
				System.out.print(listfiles(tcpConnections));
			} else if (parts[0].equalsIgnoreCase("store")) {
				if (parts.length != 2) {
					System.out.println("Use the command 'help' for usage.");
					continue;
				}
				parts[1] = parts[1].replaceFirst("^~", System.getProperty("user.home"));
				File test = new File(parts[1]);
				if (test.isFile()) {
					System.out.println("Attempting to store '" + parts[1] + "'.");
					store(storageSchema,parts[1],tcpConnections);
				} else {
					System.out.println("'" + parts[1] + "' is not a valid file.");
				}
			} else if (parts[0].equalsIgnoreCase("delete")) {
				if (parts.length != 2) {
					System.out.println("Use the command 'help' for usage.");
					continue;
				}
				delete(parts[1],tcpConnections);
			} else if (parts[0].equalsIgnoreCase("retrieve")) {
				if (parts.length != 3) {
					System.out.println("Use the command 'help' for usage.");
					continue;
				}
				parts[2] = parts[2].replaceFirst("^~", System.getProperty("user.home"));
				File test = new File(parts[2]);
				if (test.isDirectory()) {
					System.out.println("Attempting to retrieve '" + parts[1] + "' into '" + parts[2] + "'.");
					retrieve(storageSchema,parts[1],parts[2],tcpConnections);
				} else {
					System.out.println("'" + parts[2] + "' is not a valid directory.");
				}
			} else if (parts[0].equalsIgnoreCase("exit")) {
				Collection<TCPSender> values = tcpConnections.values();
				for (TCPSender sender : values) {
					sender.close();
				}
				System.out.println("Goodbye.");
				break;
			}
		}
		scanner.close();
	}
}
