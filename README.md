# Distributed File System
Creating a distributed, fault tolerant file system in Java. When it works, it should replicate data across multiple servers and repair pieces of files that have become corrupt.

## How to try it out
I've been using a Macbook running Catalina to develop this code, and as such have used SDKMAN! to install the necessary packages. *sdk current* tells me I'm using Gradle 7.4.2 and Java 17.0.2-tem, both of which work for me. I haven't tested how much leniency there is with respect to how current Gradle and Java must be to run this code, but probably I'd say having a newish version of Gradle is more important than Java. 

The *Controller*, one of the three types of nodes in the program, has been written to identify the local IP address of its machine so that it may bind to it. Since the *ChunkServer* and the *Client* are designed to communicate with the *Controller*, line 17 of 'ChunkServer.java' and line 28 of 'Client.java', each located in the 'node' subdirectory of the 'src' folder, must be modified before compilation to be the same IP to that which the *Controller* binds. You can find this IP by executing *ifconfig* in a macOS terminal window. This is what my local IP looks like: 192.168.68.59. Once those two lines of code have been correctly modified, open a terminal and navigate to the highest directory of the project, which contains the 'build', 'libs', and 'src' folders. 

I've written a script called 'osx.sh' for the project, which automates the running of the project's components. To use the script, run *./osx.sh* in the terminal window. This will use Gradle to clean and build the project. Next it will start the *Controller* node in the current terminal window, and spawn two new terminal windows. In one of the new terminal windows, run *./osx.sh*. This will, in this new window, open nine terminal tabs, and create a *ChunkServer* in each of them. Wait for new terminal tabs to stop spawning before you navigate to the last terminal window. The last terminal window will be the window used for the *Client*. The *Client* can either use **erasure coding** or **replication** as a storage technique. To use erasure coding, run *./osx.sh c erasure*, to use replication, run *./osx.sh c replication*. 

If you've followed the directions correctly, and my directions are clear, you should have open three terminal windows. One will contain the *Controller*, which will be printing out data associated with the connections it has made with the *ChunkServers*. Another will contain ten terminal tabs, nine of which are each associated with a specific *ChunkServer*. Each *ChunkServer* will print its IP address and port, along with information regarding the regular heartbeats it sends to the *Controller*. The last window will contain the *Client*, which will be waiting for a command from the user. 

The *Client* offers directions on how to it is to be used when you run the 'help' command (or when it is given a known command with improper usage).

The *Controller*, the *Client*, and the *ChunkServers* can all be run on different machines, but this script, for demo purposes, runs them locally on one machine. It does this by creating a folder for each *ChunkServer* in another folder called 'serverDirectories'. This way, any files stored on the distributed file system are contained within the project directory, albeit spread across folders in chunks or shards. After storing a file, it can be neat to watch the folders populate, or repair themselves after deletions.

## A quick overview

This project is an assignment taken from a graduate course in distributed systems at CSU. I haven't taken the course, but I took its undergraduate prerequisite during my senior year. Since I haven't attended the lectures or the recitations, I can't guarantee that my work for this projects meets the unmentioned-in-the-assignment-description-but-tested-in-their-grading requirements. This project involves a lot of code with a bunch of interacting parts, so there will be behavior that is unexpected if the right inputs are supplied.

As was briefly mentioned in the *How to try it out* section, the project is based around three types of nodes -- a node being a computer in a network executing a specific program from the project. The three types of nodes are the *Controller*, the *ChunkServer*, and the *Client*.

The *Controller* communicates with both the *Client* and the *ChunkServers*. It keeps track of the files that have been distributed over the *ChunkServers*, and at which specific server each chunk or shard resides. It is also responsible for deciding, upon receiving from the *Client* a request to store a file, which *ChunkServers* will store which portion of that file. Additionally, it receives, at regular intervals, status updates from each of the *ChunkServers* called *heartbeats*. From information contained in each heartbeat, the *Controller* must determine whether the *ChunkServer* is ripe for storage, or if it has failed. If it notices that a *ChunkServer* has failed, it will orchestrate the relocation of the lost data to another *ChunkServer*, if possible.

The *ChunkServer* is responsible for storing chunks or shards of files, sending regular heartbeat messages to the *Controller*, and serving data to the *Client*.

The *Client* communicates with the *Controller* when it wants to receive a list of the stored files, store a file, delete a file, or retrieve a file. It only interacts directly with the *ChunkServers* when it sends to them pieces of the files it wishes to store, or when it requests pieces of files for retrieval.

## Two techniques for fault tolerance

This project uses two techniques for fault tolerance, **erasure coding** and **replication**.

Erasure coding splits every file into **chunks**, which can vary in size, but for this project chunks are fixed at 64KB. A chunk is further split into nine **shards**. Six of the nine shards are *data* shards, and the other three are *parity* shards. The data shards contain, as you might expect, the {1/6}^{th} of the data of the chunk, along with a hash of that data and other identifying information. The parity shards contain between the three of them a linear algebraic representation of the data which are capable, when combined with other data and parity shards in a specific manner, of resurrecting missing or corrupt data shards. *Backblaze* has provided the code used in the project for encoding, decoding, and repairing shards with Reed-Solomon erasure coding. A detailed explanation of how they work can be found [here](https://www.backblaze.com/blog/reed-solomon/).

Replication splits every file the *Client* wishes to store into 64KB chunks. This project uses a redundancy factor of three, which means that every chunk will be stored on three *ChunkServers* at any given time. If a *ChunkServer* happens to go offline, the *Controller* will realize, from the cessation of heartbeats, that the *ChunkServer* has failed, and coordinate a relocation of that chunk to another server in order to maintain the correct replication count.
