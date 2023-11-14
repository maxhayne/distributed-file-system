import os
import json
import re
import plotly.graph_objects as go
from scipy import signal

# Append lists when we come across a new container
def appendStatLists(cpu, mem, net, io):
  cpu.append([])
  mem.append([])
  net.append([])
  io.append([])

# Normalize stats in kB, MiB, MB, B to megabytes
def normalizeStat(stat):
  stat = stat.strip()
  if "MB" in stat:
    return float(stat.split("MB")[0].strip())
  elif "MiB" in stat:
    return float(stat.split("MiB")[0].strip())
  elif "kB" in stat:
    return float(stat.split("kB")[0].strip())/1024
  elif "GB" in stat:
    return float(stat.split("GiB")[0].strip())*1024
  elif "B" in stat:
    return float(stat.split("B")[0].strip())/(1024*1024)
  else:
    return float(stat)/(1024*1024)

# Combine stats of all containers into one list
def aggregateStats(cpu, mem, net, io):
  aggregatedStats = [[],[],[],[]] #[CPU,MEM,NET,BLOCK]
  for index in range(len(cpu[0])):
    aggregatedStats[0].append(0)
    aggregatedStats[1].append(0)
    aggregatedStats[2].append(0)
    aggregatedStats[3].append(0)
    for container in range(len(cpu)):
      aggregatedStats[0][index] += cpu[container][index]
      aggregatedStats[1][index] += mem[container][index]
      aggregatedStats[2][index] += net[container][index]
      aggregatedStats[3][index] += io[container][index]
  return aggregatedStats

# Collect statistics in arrays
def collectStatistics(lines, containers, cpu, mem, net, io):
  for line in lines:
    clean = "{"+line.split("{")[1]
    j = json.loads(clean)
    if j["Name"] not in containers:
      containers.append(j["Name"])
      appendStatLists(cpu,mem,net,io)
    index = containers.index(j["Name"])
    cpu[index].append(float(j["CPUPerc"].split("%")[0]))
    mem[index].append(normalizeStat(j["MemUsage"]))
    netIO = j["NetIO"].replace("/"," ").split()
    net[index].append(normalizeStat(netIO[0])+normalizeStat(netIO[1]))
    blockIO = j["BlockIO"].replace("/"," ").split()
    io[index].append(normalizeStat(blockIO[0])+normalizeStat(blockIO[1]))

if __name__ == "__main__":
  # Parse data for replication run
  replicationLines = []
  with open("docker-replication-stats","r") as file:
    replicationLines = [line for line in file.readlines()]
  containersRep = []
  cpuRep = []
  memRep = []
  netRep = []
  ioRep = []
  collectStatistics(replicationLines, containersRep, cpuRep, memRep, netRep, ioRep)
  aggregatedReplicationStats = aggregateStats(cpuRep, memRep, netRep, ioRep)

  # Parse data for erasure run
  erasureLines = []
  with open("docker-erasure-stats","r") as file:
    erasureLines = [line for line in file.readlines()]
  containersEra = []
  cpuEra = []
  memEra = []
  netEra = []
  ioEra = []
  collectStatistics(erasureLines, containersEra, cpuEra, memEra, netEra, ioEra)
  aggregatedErasureStats = aggregateStats(cpuEra, memEra, netEra, ioEra)

  time = [i for i in range(len(cpuRep[0]))] # create time list for x-axis

  # Create plots of statistics

  # CPU USAGE
  cpuFig = go.Figure()
  cpuFig.add_trace(go.Scatter(x=time, y=aggregatedReplicationStats[0],
                      mode='lines',
                      name='replication'))
  cpuFig.add_trace(go.Scatter(x=time, y=aggregatedErasureStats[0],
                      mode='lines',
                      name='erasure-coding'))
  # Edit the layout
  cpuFig.update_layout(title='Aggregated CPU Usage Over All Nodes',
                     xaxis_title='Time (seconds)',
                     yaxis_title='CPU Usage (%)')
  # MEMORY USAGE
  memFig = go.Figure()
  memFig.add_trace(go.Scatter(x=time, y=aggregatedReplicationStats[1],
                      mode='lines',
                      name='replication'))
  memFig.add_trace(go.Scatter(x=time, y=aggregatedErasureStats[1],
                      mode='lines',
                      name='erasure-coding'))
  # Edit the layout
  memFig.update_layout(title='Aggregated Memory Usage Over All Nodes',
                     xaxis_title='Time (seconds)',
                     yaxis_title='Memory Usage (MB)')
  # NETWORK USAGE
  netFig = go.Figure()
  netFig.add_trace(go.Scatter(x=time, y=aggregatedReplicationStats[2],
                      mode='lines',
                      name='replication'))
  netFig.add_trace(go.Scatter(x=time, y=aggregatedErasureStats[2],
                      mode='lines',
                      name='erasure-coding'))
  # Edit the layout
  netFig.update_layout(title='Aggregated Network Activity Over All Nodes',
                     xaxis_title='Time (seconds)',
                     yaxis_title='Sent and Received, Combined (MB)')

  # DISK USAGE
  ioFig = go.Figure()
  ioFig.add_trace(go.Scatter(x=time, y=aggregatedReplicationStats[3],
                      mode='lines',
                      name='replication'))
  ioFig.add_trace(go.Scatter(x=time, y=aggregatedErasureStats[3],
                      mode='lines',
                      name='erasure-coding'))
  # Edit the layout
  ioFig.update_layout(title='Aggregated Disk Activity Over All Nodes',
                     xaxis_title='Time (seconds)',
                     yaxis_title='Reads and Writes, Combined (MB)')

  cpuFig.show()
  memFig.show()
  netFig.show()
  ioFig.show()

  if not os.path.exists("images"):
    os.mkdir("images")
  cpuFig.write_image("images/cpu-comparison.png", width=1400, height=1000)
  memFig.write_image("images/mem-comparison.png", width=1400, height=1000);
  netFig.write_image("images/net-comparison.png", width=1400, height=1000);
  ioFig.write_image("images/io-comparison.png", width=1400, height=1000);
