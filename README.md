# AGRS_LAB2
MapReduce Packet Traffic Analysis Programming

# Correr na própria máquina (sem hadoop)
```
java -cp p3lite.jar:lib/* p3.runner.TrafficAnalyzer -r../http.pcap
```

# Compilar p3lite.jar
```
sudo apt-get install default-jdk
sudo apt-get install ant
cd p3-longproc
ant jar
```

# Ficheiros para testar sucess ratio
```
http://traffic.comics.unina.it/Traces/dumps/20040614_port80.11-12.dump.gz (HTTP)
http://traffic.comics.unina.it/Traces/dumps/20040614_port80.12-13.dump.gz (HTTP)
http://traffic.comics.unina.it/Traces/dumps/20040614_port80.13-14.dump.gz (HTTP)
http://traffic.comics.unina.it/Traces/dumps/20050905_port25.11-12-dump.gz (SMTP)
http://traffic.comics.unina.it/Traces/dumps/20050905_port25.12-13-dump.gz (SMTP)
http://traffic.comics.unina.it/Traces/dumps/20050905_port25.13-14-dump.gz (SMTP)
```

# Comandos úteis (no hadoop)
```
hadoop fs -rmr /user/ubuntu/*
hadoop jar p3lite.jar p3.runner.PacketCount -r/pcap/http.pcap
hadoop fs -ls /user/ubuntu/PcapPacketCount_out
hadoop fs -copyToLocal /user/ubuntu/PcapPacketCount_out/part-00000 /home/ubuntu/part-00000
```
