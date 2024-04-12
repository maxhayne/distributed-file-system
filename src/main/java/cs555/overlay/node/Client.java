package cs555.overlay.node;

import cs555.overlay.config.ApplicationProperties;
import cs555.overlay.transport.TCPConnection;
import cs555.overlay.util.ClientReader;
import cs555.overlay.util.ClientWriter;
import cs555.overlay.util.FilenameUtilities;
import cs555.overlay.util.Logger;
import cs555.overlay.wireformats.*;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client node in the DFS. It is responsible for parsing commands from the user
 * to store or retrieve files (mostly). It can parse various other commands
 * too.
 *
 * @author hayne
 */
public class Client implements Node {

  private static final Logger logger = Logger.getInstance();
  private final ConcurrentHashMap<String,ClientWriter> writers;
  private final ConcurrentHashMap<String,ClientReader> readers;
  private final Object listLock; // protects the controllerFileList
  private TCPConnection controllerConnection;
  private Path workingDirectory;
  private String[] controllerFileList;

  /**
   * Default constructor.
   */
  public Client() {
    this.writers = new ConcurrentHashMap<>();
    this.readers = new ConcurrentHashMap<>();
    this.workingDirectory = Paths.get(System.getProperty("user.dir"), "data");
    this.listLock = new Object();
  }

  /**
   * Entry point for the Client. Creates a Client using the storageType
   * specified in the 'application.properties' file, and connects to the
   * Controller. The Client is a node, but it doesn't have an active
   * ServerSocket, so it doesn't have its own values for 'host' and 'port'.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    // Establish connection with Controller
    try (Socket controllerSocket = new Socket(
        ApplicationProperties.controllerHost,
        ApplicationProperties.controllerPort)) {

      Client client = new Client();

      // Set Client's connection to Controller, start the receiver
      client.controllerConnection = new TCPConnection(client, controllerSocket);
      client.controllerConnection.start();
      logger.info("Connected to the Controller.");

      // Loop for user interaction
      client.interact();
    } catch (IOException ioe) {
      logger.error("Connection to the Controller couldn't be established. " +
                   ioe.getMessage());
      System.exit(1);
    }
  }

  @Override
  public String getHost() {
    return "N/A";
  }

  @Override
  public int getPort() {
    return -1;
  }

  @Override
  public void onEvent(Event event, TCPConnection connection) {
    switch (event.getType()) {

      case Protocol.CONTROLLER_APPROVES_FILE_DELETE:
        logger.debug("Controller approved deletion of " +
                     ((GeneralMessage) event).getMessage());
        break;

      case Protocol.CHUNK_SERVER_SERVES_FILE:
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
    ChunkServerServesFile serveMessage = (ChunkServerServesFile) event;
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
          System.out.printf("%3s%-3d%s%n", "", i, controllerFileList[i]);
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

        case "p":
        case "put":
          put(splitCommand);
          break;

        case "g":
        case "get":
          get(splitCommand);
          break;

        case "d":
        case "delete":
          requestFileDelete(splitCommand);
          break;

        case "st":
        case "stop":
          stopHelper(splitCommand);
          break;

        case "r":
        case "readers":
          showReaders();
          break;

        case "w":
        case "writers":
          showWriters();
          break;

        case "f":
        case "files":
          requestFileList();
          break;

        case "s":
        case "servers":
          requestServerList();
          break;

        case "wd":
          printWorkingDirectory(splitCommand);
          break;

        case "e":
        case "exit":
          break interactLoop;

        case "h":
        case "help":
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
  private void stopHelper(String[] command) {
    if (command.length > 1) {
      stopReader(command[1]);
      stopWriter(command[1]);
    } else {
      logger.error("No filename given. Use 'help' for usage.");
    }
  }

  /**
   * Send a request to the Controller to delete a file.
   *
   * @param command user input split by whitespace
   */
  private void requestFileDelete(String[] command) {
    synchronized(listLock) {
      if (controllerFileList == null) {
        logger.error(
            "Either no files are stored on the DFS or the file list hasn't " +
            "been retrieved from the Controller yet. Use command 'files' to " +
            "retrieve the list, then use the delete command followed by one " +
            "or more of the numbers printed to the left of the list of files" +
            ".");
        return;
      }
      if (command.length > 1) {
        GeneralMessage deleteMessage =
            new GeneralMessage(Protocol.CLIENT_REQUESTS_FILE_DELETE);
        for (int i = 1; i < command.length; ++i) {
          try {
            int fileNumber = Integer.parseInt(command[i]);
            if (fileNumber >= 0 && fileNumber < controllerFileList.length &&
                !readers.containsKey(controllerFileList[fileNumber]) &&
                !writers.containsKey(controllerFileList[fileNumber])) {
              deleteMessage.setMessage(controllerFileList[fileNumber]);
              controllerConnection.getSender()
                                  .sendData(deleteMessage.getBytes());
            }
          } catch (IOException ioe) {
            logger.error("Couldn't send Controller a delete request. " +
                         ioe.getMessage());
          } catch (NumberFormatException nfe) {
            logger.error(command[i] + " is not an integer.");
          }
        }
      } else {
        logger.error("No number given. Use 'help' for usage.");
      }
    }
  }

  /**
   * Print a list of active writers.
   */
  private void showWriters() {
    writers.forEach(
        (k, v) -> System.out.printf("%3s%3d%-2s%s%n", "", v.getProgress(), "%",
            k));
  }

  /**
   * Print a list of active readers.
   */
  private void showReaders() {
    readers.forEach(
        (k, v) -> System.out.printf("%3s%3d%-2s%s%n", "", v.getProgress(), "%",
            k));
  }

  /**
   * Sends a message request to the Controller for a list of files stored on the
   * DFS.
   */
  private void requestFileList() {
    GeneralMessage requestMessage =
        new GeneralMessage(Protocol.CLIENT_REQUESTS_FILE_LIST);
    try {
      controllerConnection.getSender().sendData(requestMessage.getBytes());
    } catch (IOException ioe) {
      logger.error("Could not send a file list request to the Controller. " +
                   ioe.getMessage());
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
   * Attempts to store a file on the DFS.
   *
   * @param command String[] of command given by user, split by space
   */
  private void put(String[] command) {
    if (command.length < 2) {
      logger.error("No file given. Use 'help' for usage.");
      return;
    }
    Path pathToFile = parsePath(command[1]);
    Boolean alreadyWriting =
        writers.searchValues(10, (w) -> w.getPathToFile().equals(pathToFile));
    if (alreadyWriting == null || !alreadyWriting) {
      String dateAddedFilename =
          addDateToFilename(pathToFile.getFileName().toString());
      ClientWriter writer =
          new ClientWriter(this, pathToFile, dateAddedFilename);
      writers.put(dateAddedFilename, writer);
      (new Thread(writer)).start();
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
   * Attempts to retrieve a file from the DFS.
   *
   * @param command String[] of command given by user, split by space
   */
  private void get(String[] command) {
    synchronized(listLock) {
      if (controllerFileList == null) {
        logger.error(
            "The file list hasn't been retrieved from the Controller yet" +
            ". Use command 'files' to retrieve the list, then use the number " +
            "to the left of the filename in your 'get' command.");
        return;
      } else if (command.length < 2) {
        logger.error("No number given. Use 'help' command.");
        return;
      }
      for (int i = 1; i < command.length; ++i) {
        int fileNumber;
        try {
          fileNumber = Integer.parseInt(command[i]);
        } catch (NumberFormatException nfe) {
          logger.error(command[i] + " isn't an integer.");
          continue;
        }
        if (fileNumber >= 0 && fileNumber < controllerFileList.length &&
            !readers.containsKey(controllerFileList[fileNumber])) {
          ClientReader reader =
              new ClientReader(this, controllerFileList[fileNumber]);
          readers.put(controllerFileList[fileNumber], reader);
          (new Thread(reader)).start();
        } else {
          logger.error(
              fileNumber + " is either a duplicate, or not a valid file.");
        }
      }
    }
  }

  /**
   * Sends request to the Controller for a list of the servers currently
   * constituting the DFS.
   */
  private void requestServerList() {
    GeneralMessage requestMessage =
        new GeneralMessage(Protocol.CLIENT_REQUESTS_SERVER_LIST);
    try {
      controllerConnection.getSender().sendData(requestMessage.getBytes());
    } catch (IOException ioe) {
      logger.error("Could not send a server list request to the Controller. " +
                   ioe.getMessage());
    }
  }

  /**
   * Prints a list of valid commands.
   */
  private void showHelp() {
    System.out.printf("%3s%-19s : %s%n", "", "p[ut] PATH/FILENAME",
        "store a local file on the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "g[et] # [#...]",
        "retrieve file(s) from the DFS");
    System.out.printf("%3s%-19s : %s%n", "", "d[elete] # [#...]",
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
    System.out.printf("%3s%-19s : %s%n", "", "e[xit]",
        "disconnect from the Controller and shutdown the Client");
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