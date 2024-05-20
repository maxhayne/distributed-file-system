package cs555.overlay.node;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.transport.TCPServerThread;
import cs555.overlay.util.ClientReader;
import cs555.overlay.util.ClientWriter;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Client node in the DFS. It is responsible for parsing commands from the user
 * to store or retrieve files (mostly). It can parse various other commands
 * too.
 *
 * @author hayne
 */
public class Client implements Node {

  private static final Logger logger = Logger.getInstance();
  private final String host;
  private final int port;
  private final ConcurrentHashMap<String,ClientWriter> writers;
  private final ConcurrentHashMap<String,ClientReader> readers;
  private final Object listLock; // protects the controllerFileList
  private TCPConnection controllerConnection;
  private Path workingDirectory;
  private String[] controllerFileList;
  private static final Pattern rangePattern =
      Pattern.compile("^[0-9]+\\.\\.[0-9]+$");

  /**
   * Default constructor.
   */
  public Client(String host, int port) {
    this.host = host;
    this.port = port;
    this.writers = new ConcurrentHashMap<>();
    this.readers = new ConcurrentHashMap<>();
    this.workingDirectory = Paths.get(System.getProperty("user.dir"), "data");
    this.listLock = new Object();
  }

  /**
   * Entry point for the Client. Creates a Client using the storageType
   * specified in the 'application.properties' file, and connects to the
   * Controller.
   *
   * @param args ignored
   */
  public static void main(String[] args) {

    // Optional command line argument
    int serverPort = args.length > 0 ? Integer.parseInt(args[0]) : 0;

    // Create server and establish connection with Controller
    try (ServerSocket serverSocket = new ServerSocket(serverPort);
         Socket controllerSocket = new Socket(
             ApplicationProperties.controllerHost,
             ApplicationProperties.controllerPort);) {

      String host = InetAddress.getLocalHost().getHostAddress();
      serverPort = serverSocket.getLocalPort();
      Client client = new Client(host, serverPort);

      // Start the TCPServerThread
      (new Thread(new TCPServerThread(client, serverSocket))).start();
      logger.info("ServerThread started at [" + host + ":" + serverPort + "]");

      // Set Client's connection to Controller, start the receiver
      client.controllerConnection = new TCPConnection(client, controllerSocket);
      client.controllerConnection.start();
      logger.info("Connected to the Controller.");

      // Loop for user interaction
      client.interact();
    } catch (IOException e) {
      logger.error("Connection to the Controller couldn't be established. " +
                   e.getMessage());
      System.exit(1);
    }
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public void onEvent(Event event, TCPConnection connection) {
    switch (event.getType()) {

      case Protocol.CONTROLLER_APPROVES_FILE_DELETE:
        logger.info("Controller approved deletion of " +
                    ((GeneralMessage) event).getMessage());
        break;

      case Protocol.SERVE_CHUNK:
        directFileToReader(event);
        break;

      case Protocol.CHUNK_SERVER_DENIES_REQUEST:
        logger.debug(
            "Request denied for " + ((GeneralMessage) event).getMessage());
        break;

      case Protocol.CONTROLLER_SENDS_FILE_LIST:
        setFileListAndPrint(event);
        break;

      case Protocol.CONTROLLER_RESERVES_SERVERS:
        notifyOfServers(event);
        break;

      case Protocol.CONTROLLER_DENIES_STORAGE_REQUEST:
        logger.info("The Controller has denied a storage request.");
        stopWriter(((GeneralMessage) event).getMessage());
        break;

      case Protocol.CONTROLLER_SENDS_STORAGE_LIST:
        notifyOfStorageInfo(event);
        break;

      case Protocol.CONTROLLER_SENDS_SERVER_LIST:
        printServerList(((GeneralMessage) event).getMessage());
        break;

      default:
        logger.debug("Event couldn't be processed. " + event.getType());
        break;
    }
  }

  /**
   * Prints the list of servers sent by the Controller.
   *
   * @param serverList a string of information about servers constituting the
   * DFS, separated by newlines.
   */
  private void printServerList(String serverList) {
    if (serverList.isEmpty()) {
      System.out.printf("%3s%s%n", "", "Controller: No servers in DFS.");
    } else {
      for (String serverInformation : serverList.split("\n")) {
        System.out.printf("%3s%s%n", "", serverInformation);
      }
    }
  }

  /**
   * Directs a file received from a ChunkServer to the ClientReader waiting for
   * chunks/shards to arrive.
   *
   * @param event message being handled
   */
  private void directFileToReader(Event event) {
    ServeChunk serveMessage = (ServeChunk) event;
    String baseFilename =
        FilenameUtilities.getBaseFilename(serveMessage.getFilename());
    ClientReader reader = readers.get(baseFilename);
    if (reader != null) {
      reader.addFile(serveMessage.getFilename(), serveMessage.getContent());
    }
  }

  /**
   * Tries to stop a ClientWriter.
   *
   * @param filename filename of writer to stop
   */
  private void stopWriter(String filename) {
    ClientWriter writer = writers.get(filename);
    if (writer != null) {
      writer.requestStop();
    }
  }

  /**
   * Tries to stop a ClientReader.
   *
   * @param filename of reader to stop
   */
  private void stopReader(String filename) {
    ClientReader reader = readers.get(filename);
    if (reader != null) {
      reader.requestStop();
    }
  }

  /**
   * Will be called when a message containing addresses of ChunkServers has been
   * received by the Client. It sets the 'servers' member inside the relevant
   * ClientWriter, and unlocks it, so it may send the chunk to those servers.
   *
   * @param event message being handled
   */
  private void notifyOfServers(Event event) {
    ControllerReservesServers message = (ControllerReservesServers) event;
    ClientWriter writer = writers.get(message.getFilename());
    if (writer != null) {
      writer.setServersAndNotify(message.getServers());
    }
  }

  /**
   * Will be called when a message containing the storage info for a particular
   * file is received by the Client. This function sets the 'servers' member for
   * the ClientReader of a particular file, thus unlocking it start reading the
   * file from the DFS. If message.getServers() is null, stops the reader.
   *
   * @param event message being handled
   */
  private void notifyOfStorageInfo(Event event) {
    ControllerSendsStorageList message = (ControllerSendsStorageList) event;
    ClientReader reader = readers.get(message.getFilename());
    if (reader != null) {
      if (message.getServers() == null) {
        reader.requestStop();
      } else {
        reader.setServersAndNotify(message.getServers());
      }
    }
  }

  /**
   * Print the list of files sent by the Controller.
   *
   * @param event message being processed
   */
  private void setFileListAndPrint(Event event) {
    ControllerSendsFileList message = (ControllerSendsFileList) event;
    synchronized(listLock) {
      controllerFileList = message.getList();
      if (controllerFileList == null) {
        System.out.printf("%3s%s%n", "", "Controller: No files stored.");
      } else {
        for (int i = 0; i < controllerFileList.length; ++i) {
          System.out.printf("%3s%d %s%n", "", i, controllerFileList[i]);
        }
      }
    }
  }

  /**
   * Loops for user input at the Client.
   */
  private void interact() {
    System.out.println(
        "Enter a command or use 'help' to print a list of commands.");
    Scanner scanner = new Scanner(System.in);
    interactLoop:
    while (true) {
      String command = scanner.nextLine();
      String[] splitCommand = command.split("\\s+");
      switch (splitCommand[0].toLowerCase()) {

        case "p", "put":
          put(splitCommand);
          break;

        case "g", "get":
          get(splitCommand);
          break;

        case "d", "delete":
          requestFileDelete(splitCommand);
          break;

        case "st", "stop":
          stopHandler(splitCommand);
          break;

        case "r", "readers":
          showReaders();
          break;

        case "w", "writers":
          showWriters();
          break;

        case "f", "files":
          requestFileList();
          break;

        case "s", "servers":
          requestServerList();
          break;

        case "wd":
          printWorkingDirectory(splitCommand);
          break;

        case "ls":
          printWorkingDirectoryContents();
          break;

        case "e", "exit":
          break interactLoop;

        case "h", "help":
          showHelp();
          break;

        default:
          logger.error("Unrecognized command. Use 'help' command.");
          break;
      }
    }
    controllerConnection.close(); // Close the controllerConnection
    System.exit(0);
  }

  /**
   * Attempts to stop either a ClientWriter or ClientReader.
   *
   * @param command user input split by whitespace
   */
  private void stopHandler(String[] command) {
    if (command.length > 1) {
      stopReader(command[1]);
      stopWriter(command[1]);
    } else {
      logger.error("No filename given. Use 'help' for usage.");
    }
  }

  /**
   * Send request(s) to the Controller to delete file(s).
   *
   * @param command user input split by whitespace
   */
  private void requestFileDelete(String[] command) {
    synchronized(listLock) {
      if (controllerFileList == null) {
        logger.error("Either no files are stored on the DFS, or you must use " +
                     "the 'files' command to populate file list.");
        return;
      } else if (command.length < 2) {
        logger.error("No file number(s) given. Use 'help' command.");
        return;
      }
      // Expand all numbers user wants to delete
      List<Integer> fileNumbers = parseFileNumbers(1, command);
      fileNumbers = fileNumbers.stream().distinct().toList();
      byte type = Protocol.CLIENT_REQUESTS_FILE_DELETE;
      GeneralMessage message = new GeneralMessage(type);
      int len = controllerFileList.length;
      for (Integer index : fileNumbers) {
        if (index >= 0 && index < len) {
          String name = controllerFileList[index];
          if (!readers.containsKey(name) && !writers.containsKey(name)) {
            message.setMessage(name);
            try {
              controllerConnection.getSender().queueSend(message.getBytes());
            } catch (IOException e) {
              logger.error("Couldn't request Controller delete " + name +
                           e.getMessage());
            }
          }
        }
      }
    }
  }

  /**
   * Print a list of active writers.
   */
  private void showWriters() {
    writers.forEach((k, v) -> {
      System.out.printf("%3s%3d%-2s%s%n", "", v.getProgress(), "%", k);
    });
  }

  /**
   * Print a list of active readers.
   */
  private void showReaders() {
    readers.forEach((k, v) -> {
      System.out.printf("%3s%3d%-2s%s%n", "", v.getProgress(), "%", k);
    });
  }

  /**
   * Sends a message request to the Controller for a list of files stored on the
   * DFS.
   */
  private void requestFileList() {
    byte type = Protocol.CLIENT_REQUESTS_FILE_LIST;
    GeneralMessage requestMessage = new GeneralMessage(type);
    try {
      controllerConnection.getSender().queueSend(requestMessage.getBytes());
    } catch (IOException e) {
      logger.error(
          "Couldn't request file list from the Controller. " + e.getMessage());
    }
  }

  /**
   * Either prints the current value of 'workingDirectory', or tries to set it
   * based on input from the user.
   *
   * @param command String[] of command from user
   */
  private void printWorkingDirectory(String[] command) {
    if (command.length > 1) {
      setWorkingDirectory(command[1]);
    }
    System.out.printf("%3s%s%n", "", workingDirectory.toString());
  }

  /**
   * Takes a string representing a possible path and converts it to a path
   * object, replacing a leading tilde with the home directory, if possible.
   *
   * @param pathString String of possible path
   * @return Path object based on the pathString
   */
  private Path parsePath(String pathString) {
    Path parsedPath;
    if (pathString.startsWith("~")) {
      pathString = pathString.replaceFirst("~", "");
      pathString = removeLeadingFileSeparators(pathString);
      parsedPath = Paths.get(System.getProperty("user.home"));
    } else {
      parsedPath = workingDirectory;
    }
    if (!pathString.isEmpty()) {
      parsedPath = parsedPath.resolve(pathString);
    }
    return parsedPath.normalize(); // clean up path
  }

  /**
   * Sets the working directory based on input from the user.
   *
   * @param workdir new desired working directory
   */
  private void setWorkingDirectory(String workdir) {
    workingDirectory = parsePath(workdir);
  }

  /**
   * Removes leading file separators from a String. Helps to make sure calls
   * like 'pwd ~/' and 'pwd ~' and 'pwd ~///' all evaluate to the home
   * directory.
   *
   * @param directory String to be modified
   * @return string leading file separators removed
   */
  private String removeLeadingFileSeparators(String directory) {
    while (!directory.isEmpty() && directory.startsWith(File.separator)) {
      directory = directory.substring(1);
    }
    return directory;
  }

  /**
   * Attempts to write file(s) to the DFS.
   *
   * @param command String[] of command given by user, split by space
   */
  private void put(String[] command) {
    if (command.length < 2) {
      logger.error("No files given. Use 'help' for usage.");
      return;
    }
    for (int i = 1; i < command.length; ++i) {
      List<Path> filesFromPath = getFilesFromPath(command[i]);
      makeWriters(filesFromPath);
    }
  }

  /**
   * If pathString is a directory, returns a list of valid paths in the
   * directory. If pathString is a valid file, returns a list of size one
   * containing that path.
   *
   * @param pathString path string to expand
   * @return list of paths
   */
  private List<Path> getFilesFromPath(String pathString) {
    Path path = parsePath(pathString);
    boolean isDirectory = Files.isDirectory(path);
    boolean isFile = Files.isRegularFile(path);
    List<Path> paths = new ArrayList<>();
    if (isDirectory) {
      paths.addAll(listDirectoryFiles(path));
    } else if (isFile) {
      paths.add(path);
    }
    return paths;
  }

  /**
   * Creates a list of all non-directory, non-hidden file paths in the dirPath.
   *
   * @param dirPath path to a directory
   * @return list of file paths in directory
   */
  private List<Path> listDirectoryFiles(Path dirPath) {
    List<Path> filePaths = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath)) {
      for (Path p : stream) {
        if (Files.isRegularFile(p) && !Files.isHidden(p)) {
          filePaths.add(p);
        }
      }
    } catch (IOException e) {
      logger.error("Exception while traversing directory. " + e.getMessage());
    }
    return filePaths;
  }

  /**
   * Creates a new writer for every non-duplicate entry in the list of paths.
   *
   * @param pathsToWrite paths to create writers for
   */
  private void makeWriters(List<Path> pathsToWrite) {
    for (Path p : pathsToWrite) {
      String dateAdded = addDateToFilename(p.getFileName().toString());
      ClientWriter writer = new ClientWriter(this, p, dateAdded);
      Boolean isDuplicate;
      isDuplicate = writers.search(100, (k, v) -> v.getPathToFile().equals(p));
      if (isDuplicate == null || !isDuplicate) {
        writers.put(dateAdded, writer);
        (new Thread(writer)).start();
      }
    }
  }

  /**
   * Add timestamp to filename before last dot. Won't work well for '.tar .gz',
   * but will for most other extensions.
   *
   * @param filename filename to be modified
   * @return modified filename
   */
  private String addDateToFilename(String filename) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HHmmss");
    int fileExtensionIndex = filename.lastIndexOf('.');
    if (fileExtensionIndex != -1) {
      return filename.substring(0, fileExtensionIndex) + "_" +
             sdf.format(new Date()) + filename.substring(fileExtensionIndex);
    } else {
      return filename + "_" + sdf.format(new Date());
    }
  }

  /**
   * Attempts to read file(s) from the DFS.
   *
   * @param command String[] of command given by user, split by space
   */
  private void get(String[] command) {
    synchronized(listLock) {
      if (controllerFileList == null) {
        logger.error("Either no files are stored on the DFS, or you must use " +
                     "the 'files' command to populate file list.");
        return;
      } else if (command.length < 2) {
        logger.error("No file number(s) given. Use 'help' command.");
        return;
      }
      // Expand all numbers user wants to read
      List<Integer> fileNumbers = parseFileNumbers(1, command);
      int len = controllerFileList.length;
      for (Integer index : fileNumbers) {
        if (index >= 0 && index < len) {
          String filename = controllerFileList[index];
          if (!readers.containsKey(filename)) {
            ClientReader reader = new ClientReader(this, filename);
            readers.put(filename, reader);
            (new Thread(reader)).start();
          }
        }
      }
    }
  }

  /**
   * Reads ranges of file numbers given by user in command, and expands them
   * into a list.
   *
   * @param startIndex index in command from which to start parsing
   * @param command user command
   * @return list of file numbers
   */
  private List<Integer> parseFileNumbers(int startIndex, String[] command) {
    List<Integer> fileNumbers = new ArrayList<>();
    for (int i = startIndex; i < command.length; ++i) {
      fileNumbers.addAll(parseRange(command[i]));
    }
    return fileNumbers;
  }

  private List<Integer> parseRange(String range) {
    if (rangePattern.matcher(range).matches()) {
      String[] split = range.split("\\.\\.");
      int lower = Integer.parseInt(split[0]);
      int upper = Integer.parseInt(split[1]);
      logger.debug(
          range + " matches the range pattern! " + lower + " " + upper);
      return expandRange(lower, upper);
    }
    try {
      return new ArrayList<>(List.of(Integer.parseInt(range)));
    } catch (NumberFormatException e) {
      logger.error(range + " is not a valid number or range.");
    }
    return new ArrayList<>();
  }

  private List<Integer> expandRange(int lower, int upper) {
    List<Integer> expansion = new ArrayList<>();
    if (lower <= upper) {
      for (int i = lower; i <= upper; ++i) {
        expansion.add(i);
      }
    }
    return expansion;
  }

  /**
   * Sends request to the Controller for a list of the servers currently
   * constituting the DFS.
   */
  private void requestServerList() {
    byte type = Protocol.CLIENT_REQUESTS_SERVER_LIST;
    GeneralMessage request = new GeneralMessage(type);
    try {
      controllerConnection.getSender().queueSend(request.getBytes());
    } catch (IOException e) {
      logger.error(
          "Couldn't send server request to the Controller. " + e.getMessage());
    }
  }

  public void printWorkingDirectoryContents() {
    // ANSI escape code constants for text colors
    String RESET = "\u001B[0m";
    String BLUE = "\u001B[34m";
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(
        workingDirectory)) {
      for (Path p : stream) {
        if (Files.isDirectory(p) && !Files.isHidden(p)) {
          System.out.println("  " + BLUE + p.getFileName().toString() + RESET);
        } else if (Files.isRegularFile(p) && !Files.isHidden(p)) {
          System.out.println("  " + p.getFileName().toString());
        }
      }
    } catch (IOException e) {
      logger.info(
          "Error encountered traversing the workdir. " + e.getMessage());
    }
  }

  /**
   * Prints a list of valid commands.
   */
  private void showHelp() {
    System.out.printf("%3s%-19s : %s%n", "", "p[ut] PATH...",
        "store local file(s) on the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "g[et] #[..#]...",
        "retrieve file(s) from the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "d[elete] #[..#]...",
        "request that file(s) be deleted from the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "st[op] FILENAME",
        "tries to stop writer or reader for FILENAME");
    System.out.printf("%3s%-19s : %s%n", "", "w[riters]",
        "display list files in the process of being stored");
    System.out.printf("%3s%-19s : %s%n", "", "r[eaders]",
        "display list files in the process of being retrieved");
    System.out.printf("%3s%-19s : %s%n", "", "f[iles]",
        "print a list of files stored on the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "s[ervers]",
        "print the list of servers constituting the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "wd [NEW_WORKDIR]",
        "print the current working directory or change it");
    System.out.printf("%3s%-19s : %s%n", "", "ls",
        "print the contents of the working directory");
    System.out.printf("%3s%-19s : %s%n", "", "e[xit]",
        "disconnect from the Controller and shut down the Client");
    System.out.printf("%3s%-19s : %s%n", "", "h[elp]",
        "print a list of valid commands");
  }

  /**
   * Remove a writer from the ClientWriter hashmap. Writers call this at the end
   * of their run.
   *
   * @param pathToFile filename key of writer
   */
  public void removeWriter(String pathToFile) {
    writers.remove(pathToFile);
  }

  /**
   * Remove a reader from the ClientReader hashmap. Readers call this at the end
   * of their run.
   *
   * @param filename filename key of writer
   */
  public void removeReader(String filename) {
    readers.remove(filename);
  }

  /**
   * Getter for the controllerConnection.
   *
   * @return controllerConnection
   */
  public TCPConnection getControllerConnection() {
    return controllerConnection;
  }
}