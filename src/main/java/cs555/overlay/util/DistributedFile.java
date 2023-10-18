package cs555.overlay.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DistributedFile {

  public String filename;
  public Map<Integer, ArrayList<Chunk>> chunks;
  // sequence number maps to vector of servers which store replications
  public Map<Integer, ArrayList<Shard>> shards;
  // sequence number maps to vector of servers which store shards

  public DistributedFile(String filename) {
    this.filename = filename;
    this.chunks = new HashMap<Integer, ArrayList<Chunk>>();
    this.shards = new HashMap<Integer, ArrayList<Shard>>();
  }

  public void clear() {
    for ( ArrayList<Chunk> chunkList : chunks.values() ) {
      if ( chunkList != null ) {
        chunkList.clear();
      }
    }
    for ( ArrayList<Shard> shardList : shards.values() ) {
      if ( shardList != null ) {
        shardList.clear();
      }
    }
    chunks.clear();
    shards.clear();
  }
}