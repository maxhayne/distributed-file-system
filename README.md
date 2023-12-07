# Distributed File System
This project is an assignment from a Distributed Systems course offered through my local university. I wanted to improve my understanding of distributed applications and thought this would be a good place to start.

## A quick overview
The project focuses on three types of nodes -- a node being a computer on a network performing a specific task. They are the *Controller*, the *ChunkServer*, and the *Client*.

The *Controller* communicates with the *Client* and *ChunkServers*. It keeps track of which files are stored by which *ChunkServers*. It is also responsible for deciding, after receiving a request from the *Client* to store a file, which *ChunkServers* will be responsible for storing that file's 64KB chunks. It also at regular intervals receives status updates from the *ChunkServers* called *heartbeats*. From these heartbeats the *Controller* determines whether a *ChunkServer* is healthy or is failing. If it concludes that a *ChunkServer* has failed, it orchestrates the relocation of that *ChunkServer's* chunks to other healthy *ChunkServers*, if possible.

The *ChunkServer* is responsible for storing chunks, sending heartbeat messages, and serving chunks to the *Client*. It stores its chunks in the */tmp* directory, in a folder it creates after successfully registering called *ChunkServer-#* (the # being its identifier).

The *Client* is responsible for taking commands from the user, and storing and retrieving files from the DFS. Storage and retrieval operations are complex and require coordinated communication between both the *Controller* and the *ChunkServers*. All files read from the DFS are stored by the *Client* in a folder it creates called *reads*, located in the directory where the *Client* was run from.

## Two techniques for fault-tolerance
This project uses two techniques to achieve fault-tolerance -- **erasure coding** and **replication**.

*Replication* splits every file into 64KB **chunks**. This project uses a replication factor of three by default, meaning that at any given time, there are three copies of every chunk stored on the DFS. If a *ChunkServer* goes offline or fails to send regular heartbeats for some time, the *Controller* notices, and attempts to forward the lost chunks to other available servers. The *Controller* tries its best to ensure that three copies of every chunk exist on the DFS.

In addition to splitting every file into chunks, *erasure coding* further fragments each chunk into nine **shards**. Six of the nine hold **data**, and three of the nine hold **parity information**. Each data shard contains one-sixth of the chunk's data. The parity shards contain matrix representations of the data which, when combined with other shards in a specific manner, make it possible to recover missing or corrupt data shards. *Backblaze* has provided the code used for encoding, decoding, and recovering shards using Reed-Solomon erasure coding. To learn more about how it works click [here](https://www.backblaze.com/blog/reed-solomon/). Under erasure coding, the *Controller* strives to keep all nine fragments resident on different servers. Of the nine fragments, any six can be used to recover the content of the encoded chunk. Therefore, three servers can go offline while still maintaining availability.

Both *replication* and *erasure coding* schemas are additionally protected against data corruption with the SHA-1 hashing algorithm. Under replication, every chunk is stored on disk with eight 20-byte SHA-1 hashes, one hash for every 8KB *slice* of the chunk. During a read operation, all slices read from storage are hashed again, and those hashes are compared to the hashes already stored on disk. If the corresponding hashes don't match, the read fails and the *Controller* is notified. After being notified, the *Controller* attempts to repair the corrupt chunk by rebuilding it with healthy slices located on other *ChunkServers*.

All fragments (shards) written to disk under erasure coding are also hashed, and like under replication, any read operation where the hashes don't match prompts the *ChunkServer* to notify the *Controller* of corruption.

## How to use it
I've used *SDKMAN!* to install packages like *gradle* and *java*. *sdk current* reports that I'm using *gradle 8.1.1* and *java 17.0.8.1-tem*. I haven't compiled the project using any other versions, so if you're not using these, you'll just have to test for yourself.

First little context for the *Controller*. The *Controller's* ServerSocket binds to the host of the computer it is running on. Since the *ChunkServers* and the *Client* are designed to communicate with the *Controller*, they first have to know how to find it. They read the *application.properties* file stored in the *config* folder of the project directory for the *host* and *port* they should use to connect to the *Controller*. The file itself provides instructions for what *controllerHost* should be set to based on your environment. If you're compiling and running the project on a single machine, using either the *osx.sh* or *ubuntu.sh* script, set *controllerHost* to *localhost*. If you're compiling and running the project with Docker, set *controllerHost* to *controller*. And, if you're compiling and running the project in a distributed environment, set *controllerHost* to the hostname of the computer the *Controller* will be running on. *controllerPort* is also configurable. Set *storageType* to *replication* if you'd like to use *replication*, and to *erasure* if you'd like to use *erasure coding* as your storage schema. Setting *logLevel* to *info* will print fewer log messages. The *debug* option I used primarily for development.

The scripts *osx.sh* and *ubuntu.sh* each do the same thing, but the former is intended for macOS and the latter for Ubuntu. Each script compiles the project with gradle, and starts the *Controller* node in the currently-open terminal window. Two new terminal windows are then spawned. Executing the command *./osx.sh c* (or *./ubuntu.sh c*) in one of the new windows will start the *Client*. Executing *./osx.sh* (or *./ubuntu.sh*) in the other will start nine *ChunkServers* in nine different terminal tabs. The number of *ChunkServers* to launch can be configured in the scripts.

Commands can then be issued to each of the running nodes. The *Client*, by default, uses the *data* folder in the project directory as its working directory (which can be subsequently changed with the *wd* command). The *data* folder contains three files -- *small.txt*, *medium.pdf*, and *large.jpg* -- which I've been using for testing purposes. The command *put small.txt* should work immediately at the *Client*. You can then retrieve the file you just stored by issuing a *files* command, followed by a *get 0* command (which retrieves the zeroth file stored on the DFS).

Additionally, all nodes print usage instructions for their commands with *help*.

To use Docker instead of the scripts, change *controllerHost* in the *application.properties* file to *controller*, navigate to the *docker* folder in the project directory using your terminal, and, assuming Docker is already installed and running, use command *docker compose build* to first build the project, then *docker compose up -d* to run the project in detached mode. Then, to attach to any of the running containers, use *docker container attach <container_name>*. Detach from the the container using *CTRL-p-CTRL-q* (no dashes), which is the [escape sequence](https://docs.docker.com/engine/reference/commandline/attach/). To shut down the session, use *docker compose down*.

*Note*: Reasonably large files (a few GB) *can* be stored on the DFS, but *retrieving* those very large files won't work unless the *Client's* JVM has access to enough RAM to hold the entire file in memory. To get around the problem, I'll need to batch the retrievals and writes.

## Comparing Erasure Coding and Replication
To better understand the differences between the two storage schemas, I used the testing script *docker-generate-stats.sh* (located in the *docker* folder) to spin up two *docker compose* sessions, one using *replication* and the other using *erasure coding*. I used *docker stats* to collect statistics about each of these sessions. In each *compose* session, the *Client* stores and retrieves the three files located in the *data* folder before shutting down. I then used the script *plot-stats.py* (also located in the *docker* folder) to aggregate the CPU, MEMORY, NETWORK, and DISK usages for all containers for each session and plot the results.
### CPU Usage
![CPU usage comparison between Erasure Coding and Replication](https://github.com/maxhayne/distributed-file-system/blob/main/docker/images/cpu-comparison.png)
Erasure coding is more cpu-intensive than replication. The command to store the files is issued at the five second mark during the testing script, where the first spikes in cpu-usage appear. Under erasure coding, every chunk the *Client* stores must first be encoded into nine shards. The three test files -- *small.txt*, *medium.pdf*, and *large.jpg* -- are a combined 8.4MB in size. $`8.4MB/64KB \approx 134`$ is the total number of chunks needed to store those files. So, the calls to the *encode* function add up at scale to produce visibly larger spikes in cpu-usage over replication. The same number of calls to the *decode* function must to be made during the download as well. The total number of threads that need to be spawned is increased under erasure coding too, as the encoded data must be relayed between all nine *ChunkServers* in the system, and sending and receiving messages doesn't come free.
### Network Usage
![Network usage comparison between Erasure Coding and Replication](https://github.com/maxhayne/distributed-file-system/blob/main/docker/images/net-comparison.png)
Network activity is also greater for erasure coding than for replication. The activity shown in this plot is *cumulative* (not MB/s), so the height on the y-axis reached by the rightmost datapoint represents the total network activity for all nodes for the duration of the test script. It should be noted that since *all* messages sent are successfully received, the y-coordinate is, in a sense, a double-count of the actual data transferred over the network. Under both replication and erasure coding, the *Client* sends a chunk to be stored on the DFS only to the *first* *ChunkServer* responsible for storing that chunk. Then, it is *that* *ChunkServer*'s responsibility to forward the chunk to the next *ChunkServer*. Under replication, the chunk is only sent over the network three times. But under erasure coding, shards are sent over the network nine times. Even when already-stored shards are removed from the message before its being relayed, the total network transfer for storing a chunk using erasure coding is greater than for replication. Shards come to be ~11KB ($`64KB/6`$) in size, but because there are nine of them, the total data transferred for one chunk during a storage operation is $`\sum\nolimits_{i=1}^{9} i*11KB \approx 495KB`$, while the total under replication is $`\sum\nolimits_{i=1}^{3} 64KB = 192KB`$. These calculations are proven by the plot, as $`160MB/70MB \approx 495KB/192KB`$.
### Memory Usage
![Memory usage comparison between Erasure Coding and Replication](https://github.com/maxhayne/distributed-file-system/blob/main/docker/images/mem-comparison.png)
Memory usage is much the same story. Under erasure coding more messages need to be sent and received, and the objects used to represent those messages take up space (before they are garbage collected). The additional threads also take up more memory, though their impact is probably minimal in comparison to the message objects.
### Disk Activity
![Disk activity comparison between Erasure Coding and Replication](https://github.com/maxhayne/distributed-file-system/blob/main/docker/images/io-comparison.png)
Only in disk activity does erasure coding prevail over replication. As mentioned in the discussion of the *network* plot, an encoded chunk has a total size of ~100KB ($`9*11KB`$). Though erasure coding more heavily taxes the network during a storage operation, there is *only* 100KB of data to write to disk across all nine shards, whereas under replication, the full 64KB chunk is written to disk at three different *ChunkServers*. Replication appears to have a total disk activity of ~28.5MB for the test script, while erasure coding has a total of ~20MB. When running *docker stats* while the script is executing, I noticed a bug, in that the input (I in I/O) remains at zero throughout the run (though I know for certain that reads are being performed). To account for this, if we subtract 8.4MB from each's total (to ignore the write performed at the *Client*), $`Replication/Erasure = 20MB/12MB \approx 172KB/100KB`$. This seems reasonable.

The CPU, MEMORY, and NETWORK plots turn out similarly on successive runs, but the DISK plot doesn't. Sometimes the replication line starts at 40MB, sometimes at zero, other times the total disk activities vary between tests. I'm not sure what's going on, but it *certainly* isn't a problem with my code. *My* little python script is *bulletproof production code*.
