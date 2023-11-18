package cs555.overlay.wireformats;

public interface Protocol {
  // REGISTERING CHUNK SERVERS
  byte CHUNK_SERVER_SENDS_REGISTRATION = 1;
  byte CONTROLLER_REPORTS_CHUNK_SERVER_REGISTRATION_STATUS = 2;
  byte CHUNK_SERVER_SENDS_DEREGISTRATION = 3;

  // STORING, DELETING CHUNKS AND SHARDS
  byte CLIENT_STORE = 4;
  byte CONTROLLER_DENIES_STORAGE_REQUEST = 5;
  byte CONTROLLER_RESERVES_SERVERS = 6;
  byte CLIENT_REQUESTS_FILE_DELETE = 7;
  byte CONTROLLER_APPROVES_FILE_DELETE = 8;
  byte CONTROLLER_REQUESTS_FILE_DELETE = 9;
  byte CHUNK_SERVER_ACKNOWLEDGES_FILE_DELETE = 10;

  // SENDING DATA FOR STORAGE
  byte SENDS_FILE_FOR_STORAGE = 11;
  byte CHUNK_SERVER_ACKNOWLEDGES_FILE_FOR_STORAGE = 12;

  // RETRIEVING DATA FROM STORAGE
  byte REQUEST_FILE = 13;
  byte CHUNK_SERVER_DENIES_REQUEST = 14;
  byte CHUNK_SERVER_SERVES_FILE = 15;
  byte CLIENT_REQUESTS_FILE_STORAGE_INFO = 16;

  // HEARTBEAT MESSAGES
  byte CHUNK_SERVER_SENDS_HEARTBEAT = 17;
  byte CONTROLLER_SENDS_HEARTBEAT = 18;
  byte CHUNK_SERVER_RESPONDS_TO_HEARTBEAT = 19;

  // FILE CORRUPTION
  byte CHUNK_SERVER_REPORTS_FILE_CORRUPTION = 20;
  byte CONTROLLER_SENDS_STORAGE_LIST = 21;
  byte CHUNK_SERVER_NO_STORE_FILE = 22;
  byte REPAIR_SHARD = 23;
  byte REPAIR_CHUNK = 24;

  // LISTING INFORMATION
  byte CLIENT_REQUESTS_FILE_LIST = 25;
  byte CONTROLLER_SENDS_FILE_LIST = 26;
  byte CLIENT_REQUESTS_SERVER_LIST = 27;
  byte CONTROLLER_SENDS_SERVER_LIST = 28;
}