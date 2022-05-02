# Distributed File System
Creating a distributed, fault tolerant file system in Java. When it works, it should replicate data across multiple servers and repair pieces of files that have become corrupt.

## How to try it out
I've been using a macbook running catalina to develop this code, and as such have used SDKMAN! to install the necessary packages. 'sdk current' tells me I'm using gradle 7.4.2 and java 17.0.2-tem, both of which work for me. I haven't tested how much leniency there is with respect to how current gradle and java must be to run this code, but probably I'd say having a newish version of gradle is more important than java. 

The 'Controller', one of the three node programs in the software, has been written to identify the local IP address of the machine for binding. Since the 'ChunkServer' and the 'Client' are designed to communicate with the Controller, line 17 of 'ChunkServer.java' and line 28 of 'Client.java', each located in the 'node' subdirectory of the 'src' folder must be modified before compilation to be the same IP to that which the Controller binds. You can find this IP by running 'ifconfig' (for macOS) in a terminal window. My local IP looks like this: 192.168.68.59. Once those two lines have been correctly modified, open a terminal and navigate to the highest directory of the project, which contains the 'build', 'libs', and 'src' folder. 

I've written a script called 'osx.sh' for the project, which automates the running of the project's components. To use the script, run './osx.sh' in the terminal window. This will run first use gradle to clean and build the project. Next it will start the Controller node in the current terminal window, and spawn two new terminal windows. In one of the new terminal windows, run './osx.sh'. This will, in this new window, open nine terminal tabs, and create a ChunkServer in each of them. Wait for new terminal tabs to stop being spawned before you navigate to the other spawned terminal window. The last terminal window will be the window used for the client. The client can either choose to use erasure coding as a storage technique, or replication. To use erasure coding, use './osx.sh c erasure', to use replication, use './osx.sh c replication'. 

If you've followed the directions correctly, assuming my directions are clear, you should have three open terminal windows. One will contain the Controller, which will be printing out data associated with the connections its made with the ChunkServers. Another will contain ten terminal tabs, nine of which are each associated with a specific ChunkServer. Each ChunkServer will print its IP address and port, along with information regarding the regular heartbeats it sends to the Controller. The last window will contain the Client, which will be waiting for a command. 

The Client offers directions on how to it is to be used when you use the 'help' command (or when it is given a known command with improper usage).
