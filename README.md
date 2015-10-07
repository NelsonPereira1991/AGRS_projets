# AGRS_LAB2
MapReduce Packet Traffic Analysis Programming

# Compilar p3lite.jar
```
sudo apt-get install default-jdk
sudo apt-get install ant
cd p3-longproc
ant jar
```

# Comandos úteis
```
hadoop fs -rmr /user/ubuntu/*
hadoop jar p3lite.jar p3.runner.PacketCount -r/pcap/http.pcap
hadoop fs -ls /user/ubuntu/PcapPacketCount_out
hadoop fs -copyToLocal /user/ubuntu/PcapPacketCount_out/part-00000 /home/ubuntu/part-00000
```
