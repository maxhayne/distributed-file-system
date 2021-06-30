package cs555.overlay.node;
import cs555.overlay.util.FileDistributionService;
import cs555.overlay.transport.TCPSender;
import cs555.overlay.wireformats.*;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.net.SocketException;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.Scanner;
import java.net.Socket;
import java.io.File;

public class Client {

	public static final int DATA_SHARDS = 6; 
	public static final int PARITY_SHARDS = 3; 
	public static final int TOTAL_SHARDS = 9;
	public static final int BYTES_IN_INT = 4;
	public static final int BYTES_IN_LONG = 8;
	public static final String CONTROLLER_HOSTNAME = "192.168.68.65";
	public static final int CONTROLLER_PORT = 50000;

	private static Socket getSocket(String hostname, int port, int timeout) {
		try {
			Socket socket = new Socket(hostname,port);
			socket.setSoTimeout(timeout);
			return socket;
		} catch(ConnectException ce) {
			System.out.println("Failed to connect to the Controller: " + ce);
			return null;
		} catch(UnknownHostException uhe) {
			System.out.println("Failed to connect to the Controller: " + uhe);
			return null;
		} catch(IOException ioe) {
			System.out.println("The connection to the Controller was terminated: " + ioe);
			return null;
		}
	}

	private static String listfiles() {
		String returnable = "";
		try {
			Socket socket = getSocket(CONTROLLER_HOSTNAME,CONTROLLER_PORT,1000);
			if (socket == null) return "\n";
			TCPSender connection = new TCPSender(socket);
			ClientRequestsFileList listRequest = new ClientRequestsFileList();
			connection.sendData(listRequest.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null) {
				System.out.println("The Controller didn't respond for a request of files.");
				return "\n";
			}
			ControllerSendsFileList list = new ControllerSendsFileList(reply);
			if (list.list == null) {
				return "\n";
			}
			for (String file : list.list) {
				returnable += file + "\n";
			}
			socket.close();
		} catch (SocketTimeoutException ste) {
			returnable = "Socket timed out: " + ste;
		} catch (SocketException se) {
			returnable = "Socket exception: " + se;
		} catch (IOException ioe) {
			returnable = "IOException: " + ioe;
		}
		if (returnable.equals("")) {
			return "\n";
		}
		return returnable;
	}

	// UNFINISHED
	private static String state() {
		String returnable;
		try {
			Socket socket = getSocket(CONTROLLER_HOSTNAME,CONTROLLER_PORT,1000);
			if (socket == null) return "\n";
			TCPSender connection = new TCPSender(socket);
			connection.receiveData();
			return "\n";
		} catch (SocketTimeoutException ste) {
			returnable = "Socket timed out: " + ste;
		} catch (SocketException se) {
			returnable = "Socket exception: " + se;
		} catch (IOException ioe) {
			returnable = "IOException: " + ioe;
		}
		return returnable;
	}

	private static void store(String filename) {
		Path path = Paths.get(filename);
		Path name = path.getFileName();
		String basename = name.toString();
		try {
			Socket socket = getSocket(CONTROLLER_HOSTNAME,CONTROLLER_PORT,1000);
			if (socket == null) {
				System.out.println("Couldn't establish a connection with the socket.");
				return;
			}
			TCPSender connection = new TCPSender(socket);
			int index = 0;
			boolean finished = false;
			while (!finished) {
				byte[] newchunk = FileDistributionService.getNextChunkFromFile(filename,index);
				if (newchunk == null) finished = true;
				if (!finished) {
					ClientRequestsStoreChunk request = new ClientRequestsStoreChunk(basename,index);
					connection.sendData(request.getBytes());
					byte[] data = connection.receiveData();
					if (data == null) {
						System.out.println("No message received from Controller for chunk " + index + ".");
						break;
					} else if (data[0] == Protocol.CONTROLLER_DENIES_STORAGE_REQUEST) {
						System.out.println("The Controller denied the storage request of chunk " + index + ".");
						break;
					}
					ControllerSendsClientValidChunkServers response = new ControllerSendsClientValidChunkServers(data);
					// Now need to send the newchunk to the first available Chunk Server in the list.
					boolean sentToServer = false;
					int serverindex = 0;
					for (int i = 0; i < response.servers.length; i++) {
						String hostname = response.servers[i].split(":")[0];
						int port = Integer.valueOf(response.servers[i].split(":")[1]);
						try {
							Socket chunkServerSocket = getSocket(hostname,port,1000);
							TCPSender chunkSender = new TCPSender(chunkServerSocket);
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
							chunkSender.sendData(storeChunk.getBytes());
							byte[] storeResponse = chunkSender.receiveData();
							if (storeResponse == null) { // Try the next server
								continue;
							}
							ChunkServerAcknowledgesFileForStorage acknowledge = new ChunkServerAcknowledgesFileForStorage(storeResponse);
							System.out.println(response.servers[i] + " acknowledged the store request for '" + chunkFilename + "'.");
							sentToServer = true;
							break;
						} catch (SocketTimeoutException ste) {
						} catch (SocketException se) {
						} catch (IOException ioe) {
						}
					}
					if (sentToServer == false) break;
				}
				index++;
			}
			if (!finished) {
				// Request to delete the file from the controller
				ClientRequestsFileDelete delete = new ClientRequestsFileDelete(basename);
				connection.sendData(delete.getBytes());
				byte[] deleteResponse = connection.receiveData();
				if (deleteResponse[0] == Protocol.CONTROLLER_APPROVES_FILE_DELETE) {
					System.out.println("The storage operation was unsuccessful. Controller approved the deletion of the file.");
				} else {
					System.out.println("The storage operation was unsuccessful. Controller didn't respond to a delete request.");
				}
				return;
			}
			System.out.println("The storage operation was successful.");
			socket.close();
			connection = null;
		} catch (SocketTimeoutException ste) {
			System.out.println("Socket timed out: " + ste);
		} catch (SocketException se) {
			System.out.println("Socket exception: " + se);
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe);
		}
	}

	private static void delete(String filename) {
		Path path = Paths.get(filename);
		Path name = path.getFileName();
		String basename = name.toString();
		try {
			Socket socket = getSocket(CONTROLLER_HOSTNAME,CONTROLLER_PORT,1000);
			if (socket == null) {
				System.out.println("Couldn't establish a connection with the socket.");
				return;
			}
			TCPSender connection = new TCPSender(socket);
			ClientRequestsFileDelete delete = new ClientRequestsFileDelete(basename);
			connection.sendData(delete.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null) {
				System.out.println("The Controller didn't respond to the delete request for file '" + basename + "'.");
			} else if (reply[0] == Protocol.CONTROLLER_APPROVES_FILE_DELETE) {
				System.out.println("The Controller has acknowledged the request to delete file '" + basename + "'.");
			}
			socket.close();
			connection = null;
		} catch (SocketTimeoutException ste) {
			System.out.println("Socket timed out: " + ste);
		} catch (SocketException se) {
			System.out.println("Socket exception: " + se);
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe);
		}
	}

	private static void retrieve(String filename, String location) {
		Path path = Paths.get(filename);
		Path name = path.getFileName();
		String basename = name.toString();
		if (!location.endsWith("/")) location += "/";
		File test = new File(location+basename);
		if (test.exists()) {
			System.out.println("'" + location + basename + "' already exists. This operation will append it.");
		}
		test = null;
		try {
			Socket socket = getSocket(CONTROLLER_HOSTNAME,CONTROLLER_PORT,1000);
			if (socket == null) {
				System.out.println("Couldn't establish a connection with the socket.");
				return;
			}
			TCPSender connection = new TCPSender(socket);
			ClientRequestsFileSize size = new ClientRequestsFileSize(basename);
			connection.sendData(size.getBytes());
			byte[] reply = connection.receiveData();
			if (reply == null) {
				System.out.println("The Controller didn't respond to the size request for file '" + basename + "'.");
				return;
			}
			ControllerReportsFileSize reportedsize = new ControllerReportsFileSize(reply); // read for total chunks
			System.out.println("The total number of chunks in '" + basename + "' is " + reportedsize.totalchunks + ".");
			boolean finished = false;
			int lastchunk = 0;
			// Loop here for every chunk that needs retrieving
			for (int i = 0; i < reportedsize.totalchunks; i++) {
				String chunkName = basename + "_chunk" + String.valueOf(i);
				ClientRequestsFileStorageInfo infoRequest = new ClientRequestsFileStorageInfo(chunkName);
				connection.sendData(infoRequest.getBytes());
				reply = connection.receiveData();
				if (reply == null) {
					System.out.println("The Controller didn't respond to the info request for '" + chunkName + "'.");
					return;
				}
				ControllerSendsStorageList storageList = new ControllerSendsStorageList(reply);
				byte[] download = null;
				for (String server : storageList.replicationservers) { // Query the replication servers first
					String hostname = server.split(":")[0];
					int port = Integer.valueOf(server.split(":")[1]);
					try {
						Socket chunkServerSocket = getSocket(hostname,port,1000);
						TCPSender sender = new TCPSender(chunkServerSocket);
						RequestsChunk request = new RequestsChunk(chunkName);
						sender.sendData(request.getBytes());
						reply = sender.receiveData();
						if (reply == null) {
							chunkServerSocket.close();
							continue;
						}
						if (reply[0] == Protocol.CHUNK_SERVER_DENIES_REQUEST) {
							System.out.println(server + " has denied the request for " + chunkName + ". Moving on to next server.");
							continue;
						}
						ChunkServerServesFile serve = new ChunkServerServesFile(reply);
						download = serve.filedata;
						break;
					} catch (SocketTimeoutException ste) {
					} catch (SocketException se) {
					} catch (IOException ioe) {
					}
				}
				// Try to reconstruct from shards if download is null
				if (download == null) {
					System.out.println("Couldn't download the chunk from the replication servers. Add some logic to check for shards here.");
					/*
					int count = 0;
					for (String server : storageList.shardservers) {
						if (!server.equals("-1"))
							count++;
					}
					if (count < DATA_SHARDS) {

					}
					*/
					break;
				}
				// If download is still null, shard reconstruction didn't work, so stop the download.
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
			socket.close();
			connection = null;
		} catch (SocketTimeoutException ste) {
			System.out.println("Socket timed out: " + ste);
		} catch (SocketException se) {
			System.out.println("Socket exception: " + se);
		} catch (IOException ioe) {
			System.out.println("IOException: " + ioe);
		}
	}

	public static void main(String[] args){
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("COMMAND:");
			String command = scanner.nextLine();
			String[] parts = command.split("\\s+");
			if (parts.length > 3) {
				System.out.println("No command uses more than three arguments. Use 'help' for help.");
				continue;
			}
			if (parts[0].equalsIgnoreCase("help")) {
				System.out.println("'list' -- print a list all files stored in the file system.");
				System.out.println("'state' -- print a list of all chunks/shards stored in the file system, along with their locations in the overlay.");
				System.out.println("'store' [path/FILENAME] -- store a file on the distributed file system.");
				System.out.println("'delete' [FILENAME] -- delete a file from the distributed file system.");
				System.out.println("'retrieve' [FILENAME] [path/DOWNLOAD_LOCATION] -- retrieve a file from the distributed file system and deposit it in a select location.");
				System.out.println("'exit' -- stop the program on the client's side.");
			} else if (parts[0].equalsIgnoreCase("list")) {
				System.out.print(listfiles());
			} else if (parts[0].equalsIgnoreCase("state")) {
				System.out.print(state());
			} else if (parts[0].equalsIgnoreCase("store")) {
				if (parts.length != 2) {
					System.out.println("Use the command 'help' for usage.");
					continue;
				}
				parts[1] = parts[1].replaceFirst("^~", System.getProperty("user.home"));
				File test = new File(parts[1]);
				if (test.isFile()) {
					System.out.println("Attempting to store '" + parts[1] + "'.");
					store(parts[1]);
				} else {
					System.out.println("'" + parts[1] + "' is not a valid file.");
				}
			} else if (parts[0].equalsIgnoreCase("delete")) {
				if (parts.length != 2) {
					System.out.println("Use the command 'help' for usage.");
					continue;
				}
				delete(parts[1]);
			} else if (parts[0].equalsIgnoreCase("retrieve")) {
				if (parts.length != 3) {
					System.out.println("Use the command 'help' for usage.");
					continue;
				}
				parts[2] = parts[2].replaceFirst("^~", System.getProperty("user.home"));
				File test = new File(parts[2]);
				if (test.isDirectory()) {
					System.out.println("Attempting to retrieve '" + parts[1] + "' into '" + parts[2] + "'.");
					retrieve(parts[1],parts[2]);
				} else {
					System.out.println("'" + parts[2] + "' is not a valid directory.");
				}
			} else if (parts[0].equalsIgnoreCase("exit")) {
				System.out.println("Goodbye.");
				break;
			}
		}
		scanner.close();
	}
}