package cs555.overlay.util;

import java.util.*;

public class DistributedFileCache {

  private final Map<String, DistributedFile> fileCache;

  public DistributedFileCache() {
    this.fileCache = new HashMap<String, DistributedFile>();
  }

  // Remove entries from the fileCache that have no chunks or shards
  // associated with them
  public synchronized void prune() {
    ArrayList<String> toRemove = new ArrayList<String>();
    for ( Map.Entry<String, DistributedFile> entry : fileCache.entrySet() ) {
      boolean empty = true;
      DistributedFile file = entry.getValue();
      if ( file.chunks.isEmpty() && file.shards.isEmpty() ) {
        empty = true;
      } else {
        Collection<ArrayList<Chunk>> chunkListCollection = file.chunks.values();
        for ( ArrayList<Chunk> chunkList : chunkListCollection ) {
          if ( !chunkList.isEmpty() ) {
            empty = false;
            break;
          }
        }
        Collection<ArrayList<Shard>> shardListCollection = file.shards.values();
        for ( ArrayList<Shard> shardList : shardListCollection ) {
          if ( !shardList.isEmpty() ) {
            empty = false;
            break;
          }
        }
      }
      if ( empty ) {
        toRemove.add( entry.getKey() );
      }
    }
    // Remove the empty distributed files
    for ( String filename : toRemove ) {
      fileCache.get( filename ).clear();
      fileCache.remove( filename );
    }
  }

  public synchronized String[] getFileList() {
    int size = fileCache.size();
    if ( size == 0 ) {
      return null;
    }
    String[] list = new String[size];
    Set<String> keys = fileCache.keySet();
    int index = 0;
    for ( String key : keys ) {
      list[index] = key;
      index++;
    }
    return list;
  }

  public synchronized int getFileSize(String filename) {
    DistributedFile file = fileCache.get( filename );
    if ( file == null ) {
      return 0;
    } else if ( file.chunks == null && file.shards != null ) {
      return file.shards.size();
    } else if ( file.chunks != null && file.shards == null ) {
      return file.chunks.size();
    } else if ( file.shards == null && file.chunks == null ) {
      return 0;
    } else {
      return Math.max( file.shards.size(), file.chunks.size() );
    }
  }

  public synchronized String getChunkStorageInfo(String filename,
      int sequence) {
    DistributedFile file = fileCache.get( filename );
    if ( file == null ) {
      return "|";
    }
    StringBuilder sb = new StringBuilder();
    if ( file.chunks.get( sequence ) != null &&
         !file.chunks.get( sequence ).isEmpty() ) {
      boolean added = false;
      ArrayList<Chunk> chunks = file.chunks.get( sequence );
      for ( Chunk chunk : chunks ) {
        if ( !chunk.corrupt ) { // only return healthy chunks
          sb.append( chunk.serverIdentifier ).append( "," );
          added = true;
        }
      }
      if ( added ) {
        sb.deleteCharAt( sb.length()-1 );
      }
    }
    sb.append( "|" ); // divides chunks from shards
    if ( file.shards.get( sequence ) != null &&
         !file.shards.get( sequence ).isEmpty() ) {
      ArrayList<Shard> shards = file.shards.get( sequence );
      //System.out.println("DistributedFileCache getChunkStorageInfo: Shards
      // vector length: " + shards.size());
      for ( int i = 0; i < 9; i++ ) {
        boolean found = false;
        for ( Shard shard : shards ) {
          if ( shard.fragment == i ) {
            sb.append( shard.serverIdentifier ).append( "," );
            found = true;
            break;
          }
        }
        if ( !found ) {
          sb.append( -1 ).append( "," );
        }
      }
      sb.deleteCharAt( sb.length()-1 );
    }
    return sb.toString();
  }

  // Returns all the files removed so that new homes can be found for them.
  public synchronized ArrayList<ServerFile> removeAllFilesAtServer(
      int identifier) {
    ArrayList<ServerFile> totalRemoved =
        new ArrayList<ServerFile>(); // remove duplicates if there are any
    ArrayList<Chunk> removeChunks = new ArrayList<Chunk>();
    ArrayList<Shard> removeShards = new ArrayList<Shard>();
    ArrayList<String> removeFiles = new ArrayList<String>();
    Collection<DistributedFile> files = fileCache.values();
    for ( DistributedFile file : files ) {
      // Iterate through all chunks and shards of the file and remove any
      // that are stored on the server with the identifier
      if ( file.chunks != null && !file.chunks.isEmpty() ) {
        Collection<ArrayList<Chunk>> chunkLists = file.chunks.values();
        for ( ArrayList<Chunk> chunkList : chunkLists ) {
          for ( Chunk chunk : chunkList ) {
            // Iterating through chunks
            if ( chunk.serverIdentifier == identifier ) {
              removeChunks.add( chunk );
              totalRemoved.add( chunk );
            }
          }
          for ( Chunk chunk : removeChunks ) {
            chunkList.remove( chunk );
          }
          removeChunks.clear();
        }
      }
      if ( file.shards != null && !file.shards.isEmpty() ) {
        Collection<ArrayList<Shard>> shardLists = file.shards.values();
        for ( ArrayList<Shard> shardList : shardLists ) {
          for ( Shard shard : shardList ) {
            if ( shard.serverIdentifier == identifier ) {
              removeShards.add( shard );
              totalRemoved.add( shard );
            }
          }
          for ( Shard shard : removeShards ) {
            shardList.remove( shard );
          }
          removeShards.clear();
        }
      }
      if ( (file.chunks == null || file.chunks.isEmpty()) &&
           (file.shards == null || file.shards.isEmpty()) ) {
        removeFiles.add( file.filename );
      }
    }
    for ( String file : removeFiles ) {
      removeFile( file );
    }
    removeFiles.clear();
    return totalRemoved;
  }

  public synchronized int fileCount() {
    return fileCache.size();
  }

  public synchronized void addFile(String filename) {
    if ( fileCache.containsKey( filename ) ) {
      return;
    }
    DistributedFile newFile = new DistributedFile( filename );
    fileCache.put( filename, newFile );
  }

  public synchronized void removeFile(String filename) {
    if ( !fileCache.containsKey( filename ) ) {
      return;
    }
    fileCache.get( filename ).clear();
    fileCache.remove( filename );
  }

  public synchronized void addChunk(Chunk chunk) {
    if ( !fileCache.containsKey( chunk.filename ) ) { // add file if not present
      this.addFile( chunk.filename );
    }
    DistributedFile file = fileCache.get( chunk.filename );
    if ( !file.chunks.containsKey( chunk.sequence ) ) {
      file.chunks.put( chunk.sequence, new ArrayList<Chunk>() );
    }
    file.chunks.get( chunk.sequence ).add( chunk );
  }

  public synchronized void removeChunk(Chunk chunk) {
    if ( !fileCache.containsKey( chunk.filename ) ) {
      return;
    }
    DistributedFile file = fileCache.get( chunk.filename );
    if ( !file.chunks.containsKey( chunk.sequence ) ) {
      return;
    }
    file.chunks.get( chunk.sequence )
               .remove( chunk ); // remove chunk if it exists
  }

  public synchronized void addShard(Shard shard) {
    if ( !fileCache.containsKey( shard.filename ) ) {
      this.addFile( shard.filename );
    }
    DistributedFile file = fileCache.get( shard.filename );
    if ( !file.shards.containsKey( shard.sequence ) ) {
      file.shards.put( shard.sequence, new ArrayList<Shard>() );
    }
    file.shards.get( shard.sequence ).add( shard );
  }

  public synchronized void removeShard(Shard shard) {
    if ( !fileCache.containsKey( shard.filename ) ) {
      return;
    }
    DistributedFile file = fileCache.get( shard.filename );
    if ( !file.shards.containsKey( shard.sequence ) ) {
      return;
    }
    file.shards.get( shard.sequence )
               .remove( shard ); // remove shard if it exists
  }

  public synchronized void clear() {
    Collection<DistributedFile> files = fileCache.values();
    for ( DistributedFile file : files ) {
      file.clear();
    }
    fileCache.clear();
  }

  public synchronized boolean containsChunk(Chunk chunk) {
    return fileCache.containsKey( chunk.filename ) &&
           fileCache.get( chunk.filename ).chunks.containsKey(
               chunk.sequence ) &&
           fileCache.get( chunk.filename ).chunks.get( chunk.sequence )
                                                 .contains( chunk );
  }

  public synchronized boolean containsShard(Shard shard) {
    return fileCache.containsKey( shard.filename ) &&
           fileCache.get( shard.filename ).shards.containsKey(
               shard.sequence ) &&
           fileCache.get( shard.filename ).shards.get( shard.sequence )
                                                 .contains( shard );
  }

  public synchronized void markChunkCorrupt(String filename, int sequence,
      int serverIdentifier) {
    if ( fileCache.containsKey( filename ) &&
         fileCache.get( filename ).chunks.containsKey( sequence ) &&
         fileCache.get( filename ).chunks.get( sequence ) != null ) {
      ArrayList<Chunk> list = fileCache.get( filename ).chunks.get( sequence );
      for ( Chunk chunk : list ) {
        if ( chunk.serverIdentifier == serverIdentifier ) {
          chunk.corrupt = true;
        }
      }
    }
  }

  public synchronized void markShardCorrupt(String filename, int sequence,
      int fragment, int serverIdentifier) {
    if ( fileCache.containsKey( filename ) &&
         fileCache.get( filename ).shards.containsKey( sequence ) &&
         fileCache.get( filename ).shards.get( sequence ) != null ) {
      ArrayList<Shard> list = fileCache.get( filename ).shards.get( sequence );
      for ( Shard shard : list ) {
        if ( shard.serverIdentifier == serverIdentifier &&
             shard.fragment == fragment ) {
          shard.corrupt = true;
        }
      }
    }
  }

  public synchronized void markChunkHealthy(String filename, int sequence,
      int serverIdentifier) {
    if ( fileCache.containsKey( filename ) &&
         fileCache.get( filename ).chunks.containsKey( sequence ) &&
         fileCache.get( filename ).chunks.get( sequence ) != null ) {
      ArrayList<Chunk> list = fileCache.get( filename ).chunks.get( sequence );
      for ( Chunk chunk : list ) {
        if ( chunk.serverIdentifier == serverIdentifier ) {
          chunk.corrupt = false;
        }
      }
    }
  }

  public synchronized void markShardHealthy(String filename, int sequence,
      int fragment, int serverIdentifier) {
    if ( fileCache.containsKey( filename ) &&
         fileCache.get( filename ).shards.containsKey( sequence ) &&
         fileCache.get( filename ).shards.get( sequence ) != null ) {
      ArrayList<Shard> list = fileCache.get( filename ).shards.get( sequence );
      for ( Shard shard : list ) {
        if ( shard.serverIdentifier == serverIdentifier &&
             shard.fragment == fragment ) {
          shard.corrupt = false;
        }
      }
    }
  }

  // Function to return differences between two DistributedFileCaches.
  // Will return all chunks or shards that are stored in this.fileCache
  // that are not stored in the passedCache. Will help to identify
  // ChunkServers that are down.
  public synchronized ArrayList<String> differences(
      DistributedFileCache passedCache) {
    ArrayList<String> differences = new ArrayList<>();
    for ( Map.Entry<String, DistributedFile> entry1 : fileCache.entrySet() ) {
      String filename = entry1.getKey();
      // entry1.getKey() will be the filename
      // entry1.getValue() will be the DistributedFile instance
      // Have to iterate through entry1.getValue().chunks.entrySet()
      for ( Map.Entry<Integer, ArrayList<Chunk>> entry2 :
          entry1.getValue().chunks.entrySet() ) {
        int sequence = entry2.getKey();
        // entry2.getKey() will be the chunk#
        // entry2.getValue() will be the Vector<Chunk> for the chunk#
        // entry2.getValue().contains(Chunk) will tell you if the Chunk is
        // present
        for ( Chunk chunk : entry2.getValue() ) {
          if ( !passedCache.containsChunk( chunk ) ) {
            differences.add( chunk.timestamp+","+filename+","+sequence+","+
                             chunk.serverIdentifier );
          }
        }
      }
      for ( Map.Entry<Integer, ArrayList<Shard>> entry2 :
          entry1.getValue().shards.entrySet() ) {
        int sequence = entry2.getKey();
        for ( Shard shard : entry2.getValue() ) {
          if ( !passedCache.containsShard( shard ) ) {
            differences.add( shard.timestamp+","+filename+","+sequence+","+
                             shard.serverIdentifier+","+shard.fragment );
          }
        }
      }
    }
    return differences;
  }
}