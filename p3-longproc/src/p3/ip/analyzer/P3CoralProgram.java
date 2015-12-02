package p3.ip.analyzer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import p3.common.lib.BinaryUtils;
import p3.common.lib.BitAdder;
import p3.common.lib.Bytes;
import p3.common.lib.CommonData;
import p3.hadoop.common.pcap.lib.ExtendedBytesWritable;
import p3.hadoop.common.pcap.lib.PcapRec;
import p3.hadoop.mapred.BinaryInputFormat;
import p3.hadoop.mapred.BinaryOutputFormat;
//import netlab.hadoop.packet.PcapInputFormat;
import p3.hadoop.mapred.PcapInputFormat;
import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * 
 * @author yhlee in Chungnam National University
 *  ssallys@naver.com
 */
public class P3CoralProgram {

	private static final int FLOW_RECORD_SIZE = 17+PcapRec.LEN_VAL3;//+5;
	private static final int MIN_PKT_SIZE = 42;
		
	public static boolean isFilter = false;
	public JobConf conf;

	public static class Filter{ 
		static int net = -1;	
		static int port = -1;
		static int proto = -1;
		
		public static int getNet() {
			return net;
		}
		public static void setNet(int net) {
			Filter.net = net;
		}
		public static int getPort() {
			return port;
		}
		public static void setPort(int port) {
			Filter.port = port;
		}
		public static int getProto() {
			return proto;
		}
		public static void setProto(int proto) {
			Filter.proto = proto;
		}
	}
	
	public P3CoralProgram(){
		this.conf = new JobConf();
	}
	
	public P3CoralProgram(JobConf conf){
		this.conf = conf;
	}

    /*******************************************
				COUNT function
	*******************************************/
	
	public static class Map_CountUp extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, Text>{
		int key_len = 0;
		int key_pos = 0;
		
		public void configure(JobConf conf){
			key_len = conf.getInt("pcap.record.key.len", PcapRec.LEN_IPADDR);
			key_pos = conf.getInt("pcap.record.key.pos", PcapRec.POS_SIP);			
		}
		
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
		
			byte[] eth_type = new byte[2];
			byte[] ip_ver = {0x00};	
			byte[] new_key = new byte[key_len];
			byte[] value_bytes = value.getBytes();	
			byte[] bc = new byte[4];
			String strKey = null;
			
			if(value_bytes.length<MIN_PKT_SIZE) return;			
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			
			/* ip */
			if(BinaryUtils.byteToInt(eth_type) == PcapRec.IP_PROTO) {			
				System.arraycopy(value_bytes, PcapRec.POS_IP_VER, ip_ver, 0, PcapRec.LEN_IP_VER);
				
				/* ipv4 */				
				if((BinaryUtils.byteToInt(ip_ver) & PcapRec.IPV4) == PcapRec.IPV4){
					System.arraycopy(value_bytes, key_pos, new_key, 0, new_key.length);	
					System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bc,0, PcapRec.LEN_IP_BYTES); // set byte count
					
					if (new_key.length==4) 
						strKey = CommonData.longTostrIp(Bytes.toLong(new_key));
					else
						strKey = Long.toString(Bytes.toLong(new_key));
					
					output.collect(new Text(strKey), new Text(Bytes.toInt(bc)+" "+1));					
				}				
			}
		}
	}
		
    public static class Reduce_CountUp extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                        throws IOException {

			String line = null;
			long bc = 0;
			long pc = 0;
			
			StringTokenizer token;

           while(value.hasNext()){  
        	   line = value.next().toString();	
        	   token = new StringTokenizer(line);
	    	   if(line.length()<0)  continue;		       
		       bc += Long.parseLong(token.nextToken().trim());
		       pc += Long.parseLong(token.nextToken().trim());
           }
           output.collect(key, new Text(Long.toString(bc)+" "+Long.toString(pc)));                   
        }
    }
       
	private JobConf getCountUpJobConf(String jobName, Path inFilePath, Path outFilePath){
		
	    Path Output = new Path(jobName);			
        conf.setJobName(jobName);     
        conf.setNumReduceTasks(10);       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);	       
        conf.setInputFormat(PcapInputFormat.class);        
        conf.setOutputFormat(TextOutputFormat.class);        
        conf.setMapperClass(Map_CountUp.class);
        conf.setCombinerClass(Reduce_CountUp.class);          
        conf.setReducerClass(Reduce_CountUp.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, Output);
        
        return conf;
	}
    
    public void startCount(Path inputDir, Path outputDir, long cap_start, long cap_end){// throws IOException {
        
	try {
		FileSystem fs = FileSystem.get(conf);
        JobConf countJobconf = getCountUpJobConf("CountUp", inputDir, outputDir);        
        countJobconf.setLong("pcap.file.captime.min", cap_start);
        countJobconf.setLong("pcap.file.captime.max", cap_end);
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(countJobconf))) {
          fs.delete(FileOutputFormat.getOutputPath(countJobconf), true);
        }        
		JobClient.runJob(countJobconf);	
		
		if(conf.getInt("pcap.record.sort.field", 0) > 0){
	        Path countOutputDir = FileOutputFormat.getOutputPath(countJobconf);
	        JobConf sortJobConf = getTopNJobConf("TopN", countOutputDir, outputDir);  
	        
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(sortJobConf))) {
	          fs.delete(FileOutputFormat.getOutputPath(sortJobConf), true);
	        }
	        JobClient.runJob(sortJobConf);		
		}
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

    /*******************************************

			TrafficAnalyzer function
    *******************************************/
	
	public static class Map_TrafficAnalyzer extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, Text>{


		// pcap 16; start 0;
		// Ethernet 14 bytes; start 14; 
		// IP 20 bytes; start 30; len 32; proto 39; srcIP 42; dstIP 46
		// TCP 20 bytes ; start 50; srcPort 50; dstPort 52
		int interval = 0;
		long MAPms = 0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);
			MAPms = conf.getLong("pcap.artificial.mapms", 0);
		}	
				
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		


			byte[] pkts = {0x00, 0x01};	
			byte[] eth_type = new byte[2];
			byte[] bcap_time = new byte[4];
			byte[] value_bytes = value.getBytes();			
			if(value_bytes.length<MIN_PKT_SIZE) return;			
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			if(BinaryUtils.byteToInt(eth_type) != PcapRec.IP_PROTO) return;

			System.arraycopy(value_bytes, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
			Long cap_time = new Long(0);			
			cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

			System.arraycopy(value_bytes, PcapRec.POS_TSTMP+4, bcap_time, 0, 4);	
			Long cap_time_2 = new Long(0);			
			cap_time_2 = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));
		
			byte[] ip = new byte[4];
			byte[] port = new byte[2];
			byte[] srcPort = new byte[2];
			byte[] dstPort = new byte[2];
			byte[] proto = new byte[1];
			byte[] bc = new byte[2];	
			byte[] tcpHeader = new byte[1];
			byte[] ipHeaderlength = new byte[1];
			byte[] iplength = new byte[2];
			
			
			System.arraycopy(value_bytes, PcapRec.POS_PT, proto, 0, 1);
            Long protocol = Bytes.toLong(proto);
			System.arraycopy(value_bytes, PcapRec.POS_SIP, ip, 0, 4);
            String sourceIP = CommonData.longTostrIp(Bytes.toLong(ip));
			System.arraycopy(value_bytes, PcapRec.POS_SIP+4, ip, 0, 4);
            String destinationIP = CommonData.longTostrIp(Bytes.toLong(ip));

			System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bc, 0, 2);				
			Long ibc = Bytes.toLong(bc);
			System.arraycopy(value_bytes, PcapRec.POS_HL, ipHeaderlength, 0, 1);
			
			int totalIPHeaderLength = (Bytes.toInt(ipHeaderlength) & 0xF) * 4;
			int POS_TCP = 30 + totalIPHeaderLength;
			
			System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, iplength, 0, PcapRec.LEN_IP_BYTES);
			System.arraycopy(value_bytes, PcapRec.POS_SIP+8, srcPort, 0, 2);
			System.arraycopy(value_bytes, PcapRec.POS_SIP+10, dstPort, 0, 2);
			System.arraycopy(value_bytes, POS_TCP+12, tcpHeader, 0, 1);

            Long sourcePort = Bytes.toLong(srcPort);
            Long destinationPort = Bytes.toLong(dstPort);
			
			int totalIPLength = Bytes.toInt(iplength);
			int tcpHeaderLenght = (Bytes.toInt(tcpHeader) >> 4) * 4;			
			int payloadLength = totalIPLength - totalIPHeaderLength - tcpHeaderLenght;
			//byte[] payload = new byte[payloadLength];
			//System.arraycopy(value_bytes, POS_TCP+tcpHeaderLenght, payload, 0, payloadLength);
			//String payloadText = new String(payload, "UTF-8");

            // Discard when payload length is 0 or protocol is different from TCP
            if( payloadLength == 0 || protocol != 6 ) return;	

            // Organize by flow (with key bellow)
    		output.collect(
                new Text(sourceIP+","+destinationIP+","+sourcePort+","+destinationPort+","+protocol), 
                new Text(cap_time+"."+cap_time_2+","+ibc)
            );
		}
	}
	
	public static class Reduce_TrafficAnalyzer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> {	
    	
		int interval = 0;		
		long REDms = 0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);		
			REDms = conf.getLong("pcap.artificial.redms", 0);
		}

	    public double PDNormalDistribution(double m, double d, double x ) {	
		    return 1/(d*Math.sqrt(2*Math.PI)) * 
					    Math.exp( - Math.pow((x-m), 2) / (2*Math.pow(d, 2)) );
	    }

	    public double logPDNormalDistribution(double m, double d, double x ) {	
		    return (x > 0) ? Math.log(PDNormalDistribution(m,d,x)) : 0;
	    }


        public class PacketInfo {
            public double timestamp;
            public int byteCount;

            public PacketInfo(String t, String bc) {
                this.timestamp = Double.parseDouble(t);
                this.byteCount = Integer.parseInt(bc);
            }
        }
		
	    public void reduce(Text key, Iterator<Text> value,
	                    OutputCollector<Text, Text> output, Reporter reporter)
	                   throws IOException {


            ArrayList<PacketInfo> list = new ArrayList<PacketInfo>();

            // Get values
            while( value.hasNext() ) {  
                String[] tok = value.next().toString().split(",");
                PacketInfo packet = new PacketInfo(tok[0], tok[1]);
                list.add(packet);
            }

            // Sort array
            Collections.sort(list, new Comparator<PacketInfo>() {

                @Override
                public int compare(PacketInfo c1, PacketInfo c2) {
                    return Double.compare(c1.timestamp, c2.timestamp);
                }
            });

            // ByteCount average
            double count = 0;
            for ( PacketInfo obj : list )
                count += obj.byteCount;

            double media = count/list.size();

            // Timestamp average
            count = 0;
            for (int i=1; i<list.size();i++)
            {
                count += (list.get(i).timestamp - list.get(i-1).timestamp);
            }

            double mediaTimestamp = list.size() > 1 ? (count/(list.size()-1)) : 0;
            
            // Calcular probabilidades usando NormalDistribution (metodo 1)
            double httpProb = logPDNormalDistribution(628.7194, 385.1892, media) + logPDNormalDistribution(2.584803, 56.99175, mediaTimestamp);
            double smtpProb = logPDNormalDistribution(222.4691, 282.4612, media) + logPDNormalDistribution(2.425682, 20.6584, mediaTimestamp);

            if ( httpProb > smtpProb ) {
                output.collect(key, new Text(media+","+mediaTimestamp+",HTTP"));
            } 
            else {
                output.collect(key, new Text(media+","+mediaTimestamp+",SMTP"));
            }
            
            // Calcular probabilidades usando MixtureMultivariateNormalDistribution (metodo 2)
            /*double[] val = new double[]{media, mediaTimestamp};
            double[] weights = new double[] { 0.6, 0.4 };

            // Http class
            MultivariateNormalDistribution[] mvnHttp = new MultivariateNormalDistribution[2];
            mvnHttp[0] = new MultivariateNormalDistribution(
                        new double[] { 424.217, 2.592 },
                        new double[][] {
                            { Math.pow(162.760, 2), 0 },
                            { 0, Math.pow(57.180, 2) } 
            });
            mvnHttp[1] = new MultivariateNormalDistribution(
                        new double[] { 1205.283, 2.566 }, new double[][] {
                        { Math.pow(204.101, 2), 0 },
                        { 0, Math.pow(56.458, 2) } 
            });

            // Smtp class
            MultivariateNormalDistribution[] mvnSmtp = new MultivariateNormalDistribution[2];
            mvnSmtp[0] = new MultivariateNormalDistribution(
                        new double[] { 821.138, 0.612 },
                        new double[][] {
                            { Math.pow(173.548, 2), 0 },
                            { 0, Math.pow(4.032, 2) } 
            });
            mvnSmtp[1] = new MultivariateNormalDistribution(
                        new double[] { 101.601, 2.792 }, new double[][] {
                        { Math.pow(53.341, 2), 0 },
                        { 0, Math.pow(22.558, 2) } 
            });

            // Http components
            List<Pair<Double, MultivariateNormalDistribution>> componentsHttp = new ArrayList<Pair<Double, MultivariateNormalDistribution>>();
            componentsHttp.add(new Pair<Double, MultivariateNormalDistribution>(weights[0], mvnHttp[0]));
            componentsHttp.add(new Pair<Double, MultivariateNormalDistribution>(weights[1], mvnHttp[1]));

            // Smtp components
            List<Pair<Double, MultivariateNormalDistribution>> componentsSmtp = new ArrayList<Pair<Double, MultivariateNormalDistribution>>();
            componentsSmtp.add(new Pair<Double, MultivariateNormalDistribution>(weights[0], mvnSmtp[0]));
            componentsSmtp.add(new Pair<Double, MultivariateNormalDistribution>(weights[1], mvnSmtp[1]));

            MixtureMultivariateNormalDistribution mHttp = new MixtureMultivariateNormalDistribution(componentsHttp);
            MixtureMultivariateNormalDistribution mSmtp = new MixtureMultivariateNormalDistribution(componentsSmtp);

		    //System.out.println("Densidade1: " + m1.density(val));
		    //System.out.println("Densidade2: " + m2.density(val));
		
            if ( mHttp.density(val) > mSmtp.density(val) ) {
                output.collect(key, new Text(media+","+mediaTimestamp+",HTTP"));
            } 
            else {
                output.collect(key, new Text(media+","+mediaTimestamp+",SMTP"));
            }*/
	    }
    }

	
	private JobConf getTrafficAnalyzerJobConf(String jobName, Path inFilePath, Path outFilePath){
		
		JobConf conf = new JobConf(P3CoralProgram.class);
		 
        conf.setJobName(jobName);
        conf.setNumReduceTasks(2);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);	 
        conf.setInputFormat(PcapInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setMapperClass(Map_TrafficAnalyzer.class);
        conf.setReducerClass(Reduce_TrafficAnalyzer.class);    

        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}
	    
    public void startTrafficAnalyzer(Path inputDir, Path outputDir, long cap_start, long cap_end, long MAPms, long REDms){// throws IOException {
        
	try {
		FileSystem fs = FileSystem.get(conf);
        JobConf countJobconf = getTrafficAnalyzerJobConf("TrafficAnalyzer", inputDir, outputDir);        
        countJobconf.setLong("pcap.file.captime.min", cap_start);
        countJobconf.setLong("pcap.file.captime.max", cap_end);
        
        countJobconf.setLong("pcap.artificial.mapms", MAPms);
        countJobconf.setLong("pcap.artificial.redms", REDms);
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(countJobconf))) {
          fs.delete(FileOutputFormat.getOutputPath(countJobconf), true);
        }        
		JobClient.runJob(countJobconf);	
        		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }    
    
    

    /*******************************************
			PacketCount function
    *******************************************/

	
	
	public static class Map_PacketCount extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, Text>{
		
		int interval = 0;
		long MAPms = 0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);
			MAPms = conf.getLong("pcap.artificial.mapms", 0);
		}	
		
		// pcap 16; start 0;
		// Ethernet 14 bytes; start 14; 
		// IP 20 bytes; start 30; len 32; proto 39; srcIP 42; dstIP 46
		// TCP 20 bytes ; start 50; srcPort 50; dstPort 52
		

		
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		

			
			byte[] pkts = {0x00, 0x01};	
			byte[] eth_type = new byte[2];
			byte[] bcap_time = new byte[4];
			byte[] value_bytes = value.getBytes();			
			if(value_bytes.length<MIN_PKT_SIZE) return;			
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			if(BinaryUtils.byteToInt(eth_type) != PcapRec.IP_PROTO) return;
			
			
		
			System.arraycopy(value_bytes, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
			Long cap_time = new Long(0);			
			cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

			System.arraycopy(value_bytes, PcapRec.POS_TSTMP+4, bcap_time, 0, 4);	
			Long cap_time_2 = new Long(0);			
			cap_time_2 = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));
			
			
			byte[] ip = new byte[4];
			byte[] port = new byte[2];
			byte[] srcPort = new byte[2];
			byte[] dstPort = new byte[2];
			byte[] proto = new byte[1];
			byte[] bc= new byte[2];	
			byte[] tcpHeader = new byte[1];
			byte[] ipHeaderlength = new byte[1];
			byte[] iplength = new byte[2];
			
			
			System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bc, 0, 2);				
			Long ibc = Bytes.toLong(bc);
			System.arraycopy(value_bytes, PcapRec.POS_HL, ipHeaderlength, 0, 1);
			
			int totalIPHeaderLength = (Bytes.toInt(ipHeaderlength) & 0xF) * 4;
			int POS_TCP = 30 + totalIPHeaderLength;
			
			System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, iplength, 0, PcapRec.LEN_IP_BYTES);
			System.arraycopy(value_bytes, PcapRec.POS_SIP+8, srcPort, 0, 2);
			System.arraycopy(value_bytes, PcapRec.POS_SIP+10, dstPort, 0, 2);
			System.arraycopy(value_bytes, POS_TCP+12, tcpHeader, 0, 1);
			
			int totalIPLength = Bytes.toInt(iplength);
			int tcpHeaderLenght = (Bytes.toInt(tcpHeader) >> 4) * 4;			
			int payloadLength = totalIPLength - totalIPHeaderLength - tcpHeaderLenght;
			byte[] payload = new byte[payloadLength];
			
			System.arraycopy(value_bytes, POS_TCP+tcpHeaderLenght, payload, 0, payloadLength);
			String payloadText = new String(payload, "UTF-8");
			
			if ( Pattern.compile("GET /.* HTTP/\\d.\\d").matcher(payloadText).find() )
			{
			    /*System.out.println("POS_TCP: "+POS_TCP);
			    System.out.println("totalIP: "+totalIPLength);
			    System.out.println("totalIPHeader: "+totalIPHeaderLength);
			    System.out.println("totalTCPHeader: "+tcpHeaderLenght);
				System.out.println("Payload: "+payloadText);*/
				
				output.collect(new Text("HTTP2"), new Text(""+1));
			}

		    output.collect(new Text(Long.toString(Bytes.toLong(srcPort))), new Text(""+1));
		    output.collect(new Text(Long.toString(Bytes.toLong(dstPort))), new Text(""+1));		
		}
	}
	
	public static class Reduce_PacketCount extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> {	
    	
		int interval = 0;		
		long REDms = 0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);		
			REDms = conf.getLong("pcap.artificial.redms", 0);
		}
		
	    public void reduce(Text key, Iterator<Text> value,
	                    OutputCollector<Text, Text> output, Reporter reporter)
	                   throws IOException {

            long sum = 0;

            while( value.hasNext() ) {  
                sum += 1;//Long.parseLong( value.next().toString() );
                value.next();
            }
            output.collect(key, new Text(Long.toString(sum)));
	    }
    }

	
	private JobConf getPacketCountJobConf(String jobName, Path inFilePath, Path outFilePath){
		
		JobConf conf = new JobConf(P3CoralProgram.class);
		 
        conf.setJobName(jobName);
        conf.setNumReduceTasks(2);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);	 
        conf.setInputFormat(PcapInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setMapperClass(Map_PacketCount.class);
        conf.setReducerClass(Reduce_PacketCount.class);    

        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}
	    
    public void startPacketCount(Path inputDir, Path outputDir, long cap_start, long cap_end, long MAPms, long REDms){// throws IOException {
        
	try {
		FileSystem fs = FileSystem.get(conf);
        JobConf countJobconf = getPacketCountJobConf("PacketCount", inputDir, outputDir);        
        countJobconf.setLong("pcap.file.captime.min", cap_start);
        countJobconf.setLong("pcap.file.captime.max", cap_end);
        
        countJobconf.setLong("pcap.artificial.mapms", MAPms);
        countJobconf.setLong("pcap.artificial.redms", REDms);
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(countJobconf))) {
          fs.delete(FileOutputFormat.getOutputPath(countJobconf), true);
        }        
		JobClient.runJob(countJobconf);	
		
	} catch (IOException e) {
		// TODO Auto-generated catch block			System.arraycopy(value_bytes, PcapRec.POS_PT, proto, 0, 1);
		e.printStackTrace();
	}
  }    
    

    
    
    /*******************************************
			LongProc function
    *******************************************/

	
	
	public static class Map_LongProc extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, Text>{
		
		int interval = 0;
		long MAPms = 0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);
			MAPms = conf.getLong("pcap.artificial.mapms", 0);
		}	
		
		// pcap 16; start 0;
		// Ethernet 14 bytes; start 14; 
		// IP 20 bytes; start 30; len 32; proto 39; srcIP 42; dstIP 46
		// TCP 20 bytes ; start 50; srcPort 50; dstPort 52
		

		
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		

			
			byte[] pkts = {0x00, 0x01};	
			byte[] eth_type = new byte[2];
			byte[] bcap_time = new byte[4];
			byte[] value_bytes = value.getBytes();			
			if(value_bytes.length<MIN_PKT_SIZE) return;			
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			if(BinaryUtils.byteToInt(eth_type) != PcapRec.IP_PROTO) return;
			
			
		
			System.arraycopy(value_bytes, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
			Long cap_time = new Long(0);			
			cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

			System.arraycopy(value_bytes, PcapRec.POS_TSTMP+4, bcap_time, 0, 4);	
			Long cap_time_2 = new Long(0);			
			cap_time_2 = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));
			
			
			byte[] ip = new byte[4];
			byte[] port = new byte[2];
			byte[] proto = new byte[1];
			byte[] bc= new byte[2];			
			
			
			System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bc, 0, 2);				
			Long ibc = Bytes.toLong(bc);

			
			String strTuple = "";			
			
			System.arraycopy(value_bytes, PcapRec.POS_SIP, ip, 0, 4);
			strTuple += CommonData.longTostrIp(Bytes.toLong(ip))+" ";
			System.arraycopy(value_bytes, PcapRec.POS_SIP+4, ip, 0, 4);
			strTuple += CommonData.longTostrIp(Bytes.toLong(ip))+" ";
			System.arraycopy(value_bytes, PcapRec.POS_SIP+8, port, 0, 2);
			strTuple += Bytes.toLong(port)+" ";		
			System.arraycopy(value_bytes, PcapRec.POS_SIP+10, port, 0, 2);
			strTuple += Bytes.toInt(port)+" ";	
			System.arraycopy(value_bytes, PcapRec.POS_PT, proto, 0, 1);
			strTuple += Bytes.toInt(proto)+" ";	
			
			String out = cap_time.toString() + ":" + cap_time_2.toString() + ":" + ibc.toString(); 
			
			output.collect(new Text(strTuple), new Text(out));
			
			
			long sleepTime = MAPms*1000000L; // convert to nanoseconds
			long startTime = System.nanoTime();
			while ((System.nanoTime() - startTime) < sleepTime) {}
		}
	}
	
	public static class Reduce_LongProc extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> {	
    	
		int interval = 0;		
		long REDms = 0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);		
			REDms = conf.getLong("pcap.artificial.redms", 0);
		}
		
	    public void reduce(Text key, Iterator<Text> value,
	                    OutputCollector<Text, Text> output, Reporter reporter)
	                   throws IOException {

	    	Long bc_sum = new Long(0);
	    	StringTokenizer stok; 
	    	String t1, t2, bc;
	    	//ArrayList<BigInteger> t_list = new ArrayList<BigInteger>();
	    	//ArrayList<PacketInfo> p_list = new ArrayList<PacketInfo> ();
	    	
	    	
	    	long packet_count = 0;
	    	
	    	while(value.hasNext()){  
	    			String line = value.next().toString();
	    			stok = new StringTokenizer(line,":");
	    			if(line.length()<0)  continue;
	    			t1 = stok.nextToken();
	    			t2 = stok.nextToken();
	    			bc = stok.nextToken();
	    			
	    			packet_count++;
	    			
	    			bc_sum = bc_sum + Long.parseLong(bc);
//	    			BigInteger bi = new BigInteger(t1);
//	    			bi = bi.multiply(new BigInteger("1000000"));
//	    			bi = bi.add(new BigInteger(t2));
//	    			t_list.add(bi);	    			
//	    			p_list.add(new PacketInfo(bi, Long.parseLong(bc)));
	    	}	  
	    	
//	    	BigInteger[] bi_array = t_list.toArray(new BigInteger[t_list.size()]);
//	    	java.util.Arrays.sort(bi_array);
	    	
//	    	PacketInfo[] pi_array = p_list.toArray(new PacketInfo[p_list.size()]);
//	    	java.util.Arrays.sort(pi_array);
	    	
	    	String out = bc_sum.toString() + " " + packet_count + " ";
//	    	for (int i = 1; i < bi_array.length; i++)
//	    	{
//	    		out = out + bi_array[i].subtract(bi_array[i-1]).toString() + " ";
//	    	}
	    	output.collect(key, new Text(out));  
	    	
		long sleepTime = REDms*1000000L; // convert to nanoseconds
		long startTime = System.nanoTime();
		while ((System.nanoTime() - startTime) < sleepTime) {}

	    	
	    }
    }

	
	private JobConf getLongProcJobConf(String jobName, Path inFilePath, Path outFilePath){
		
		JobConf conf = new JobConf(P3CoralProgram.class);
		 
        conf.setJobName(jobName);
        conf.setNumReduceTasks(2);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);	 
        conf.setInputFormat(PcapInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setMapperClass(Map_LongProc.class);
        conf.setReducerClass(Reduce_LongProc.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}
	    
    public void startLongProc(Path inputDir, Path outputDir, long cap_start, long cap_end, long MAPms, long REDms){// throws IOException {
        
	try {
		FileSystem fs = FileSystem.get(conf);
        JobConf countJobconf = getLongProcJobConf("LongProc", inputDir, outputDir);        
        countJobconf.setLong("pcap.file.captime.min", cap_start);
        countJobconf.setLong("pcap.file.captime.max", cap_end);
        
        countJobconf.setLong("pcap.artificial.mapms", MAPms);
        countJobconf.setLong("pcap.artificial.redms", REDms);
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(countJobconf))) {
          fs.delete(FileOutputFormat.getOutputPath(countJobconf), true);
        }        
		JobClient.runJob(countJobconf);	
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }    
    
    
    
    /*******************************************
			TotalStats function
    *******************************************/
    
	public static class Map_Stats1 extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, LongWritable>{
		
		int interval = 0;		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);			
		}	
		
		public void map		
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {		

			byte[] eth_type = new byte[2];
			byte[] ip_ver = {0x00};
			byte[] proto = new byte[1];
			byte[] bcap_time = new byte[4];
			long cap_time =0;
			byte[] ipv4 = new byte[4];
			byte[] port = new byte[2];		
			byte[] bytes = {0x00, 0x00};			
			
			byte[] value_bytes = value.getBytes();		
			if(value_bytes.length<MIN_PKT_SIZE) return;
			
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			
			/* ip */
			if(BinaryUtils.byteToInt(eth_type) == PcapRec.IP_PROTO) {			
				System.arraycopy(value_bytes, PcapRec.POS_IP_VER, ip_ver, 0, PcapRec.LEN_IP_VER);
				
				/* ipv4 */				
				if((BinaryUtils.byteToInt(ip_ver) & PcapRec.IPV4) == PcapRec.IPV4){
					/* ipv4 bc&pc */
					System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bytes, 0, PcapRec.LEN_IP_BYTES);
					int bc = Bytes.toInt(bytes);
					output.collect(new Text("IPv4 bc:"), new LongWritable(bc));
					output.collect(new Text("IPv4 pc:"), new LongWritable(1));
					
					/* ipv4 fc */				
					System.arraycopy(value_bytes, PcapRec.POS_SIP, ipv4, 0, ipv4.length);
					output.collect(new Text("IPv4 addr:" + CommonData.longTostrIp(Bytes.toLong(ipv4))+":"), new LongWritable(0));
					output.collect(new Text("IPv4 srcaddr:"+CommonData.longTostrIp(Bytes.toLong(ipv4))+":"), new LongWritable(0));	
					
					System.arraycopy(value_bytes, PcapRec.POS_DIP, ipv4, 0, ipv4.length);
					output.collect(new Text("IPv4 addr:"+ CommonData.longTostrIp(Bytes.toLong(ipv4))+":"), new LongWritable(0));
					output.collect(new Text("IPv4 dstaddr:"+CommonData.longTostrIp(Bytes.toLong(ipv4))+":"), new LongWritable(0));		
					
					System.arraycopy(value_bytes, PcapRec.POS_PT, proto, 0, proto.length);

					byte[] hlen = new byte[1];
					System.arraycopy(value_bytes, PcapRec.POS_HL, hlen, 0, hlen.length);					
					int optLen = (hlen[0] & 0x0f)*4 - 20;
					/* ICMP */					
					if(BinaryUtils.byteToInt(proto) == PcapRec.ICMP){
						System.arraycopy(value_bytes, PcapRec.ICMP_TC, port, 0, port.length);
						output.collect(new Text("ICMP type/codes:"+Bytes.toInt(port)+":"), new LongWritable(0));		
					}
					/* TCP */
					else if(BinaryUtils.byteToInt(proto) == PcapRec.TCP){					
						System.arraycopy(value_bytes, PcapRec.POS_SP+optLen, port, 0, port.length);
						output.collect(new Text("IPv4 tcp srcPort:"+Bytes.toInt(port)+":"), new LongWritable(0));	
						
						System.arraycopy(value_bytes, PcapRec.POS_DP+optLen, port, 0, port.length);
						output.collect(new Text("IPv4 tcp dstPort:"+Bytes.toInt(port)+":"), new LongWritable(0));	
					}
					/* UDP */
					else if(BinaryUtils.byteToInt(proto) == PcapRec.UDP){
						System.arraycopy(value_bytes, PcapRec.POS_SP+optLen, port, 0, port.length);
						output.collect(new Text("IPv4 udp srcPort:"+Bytes.toInt(port)+":"), new LongWritable(0));	
						
						System.arraycopy(value_bytes, PcapRec.POS_DP+optLen, port, 0, port.length);
						output.collect(new Text("IPv4 udp dstPort:"+Bytes.toInt(port)+":"), new LongWritable(0));		
					}
					
					/* flows */
					System.arraycopy(value_bytes, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
//					cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4))& interval_mask;
					cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));
					cap_time = cap_time - (cap_time % interval);
					byte[] flow = new byte[17];
					System.arraycopy(value_bytes, PcapRec.POS_SIP, flow, 0, 12);		
					System.arraycopy(value_bytes, PcapRec.POS_PT, flow, 12, 1);	
					System.arraycopy(BinaryUtils.uIntToBytes(cap_time), 0, flow, 13, 4);		
					
					output.collect(new Text("IPv4 flows:"+Bytes.toLong(flow)+":"), new LongWritable(0));				
				}
				
				/* ipv6 */	
				else{
					/* ipv6 bc */
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_BYTES, bytes, 0, bytes.length);
					int bc = Bytes.toInt(bytes);
					output.collect(new Text("IPv6 bc:"), new LongWritable(bc));
					output.collect(new Text("IPv6 pc:"), new LongWritable(1));						
				}				
			}
			/* non-ip */
			else{					
				output.collect(new Text("non-IP protocols:"+Bytes.toInt(eth_type)+":"), new LongWritable(0));	
				output.collect(new Text("non-IP pc:"), new LongWritable(1));	
			}
		}
	}
		
    public static class Reduce_Stats1 extends MapReduceBase 
    	implements Reducer<Text, LongWritable, Text, LongWritable> {	
        public void reduce(Text key, Iterator<LongWritable> value,
                        OutputCollector<Text, LongWritable> output, Reporter reporter)
                        throws IOException {
            long sum = 0;
            int new_value = 1;         	
          
           String[] tok = key.toString().split(":");          
           if(tok.length > 1){
               output.collect(key, new LongWritable(new_value));        	   
           }else{
	           while(value.hasNext()) 		 				
	        	   sum += value.next().get();
	           output.collect(key, new LongWritable(sum));        
           }
        }
    }
    
	public static class Map_Stats2 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {		

           String[] tok = value.toString().split(":");
           if(tok.length == 3){	  	   
           	   output.collect(new Text(tok[0]), new LongWritable(Long.parseLong(tok[2].trim())));
           }else if(tok.length == 2){
           	   output.collect(new Text(tok[0]), new LongWritable(Long.parseLong(tok[1].trim())));
           }
        	   
		}
	}

    public static class Reduce_Stats2 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterator<LongWritable> value,
                    OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

       long sum = 0;
       while(value.hasNext()) 		 				
    	   sum += value.next().get();
       output.collect(key, new LongWritable(sum));        
    }
}
	
	private JobConf getStatsGenJobConf(String jobName, Path inFilePath, boolean fh_skip){
		
	    Path Output = new Path(jobName);			
        conf.setJobName(jobName);     
        conf.setNumReduceTasks(20);       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	       
       	conf.setInputFormat(PcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        conf.setCombinerClass(Reduce_Stats1.class);          
        conf.setReducerClass(Reduce_Stats1.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, Output);
        
        return conf;
	}
	
	private JobConf getStatsReduceJobConf(String jobName, Path inFilePath, Path outFilePath){
					
		JobConf conf = new JobConf(P3CoralProgram.class);
        conf.addResource("p3-default.xml");
        conf.setInt("pcap.record.rate.interval", this.conf.getInt("pcap.record.rate.interval", 60));
        conf.setInt("pcap.record.key.pos", this.conf.getInt("pcap.record.key.pos", PcapRec.POS_SIP));
        conf.setInt("pcap.record.key.len", this.conf.getInt("pcap.record.key.len", PcapRec.LEN_IPADDR));
        conf.setInt("pcap.record.sort.field", this.conf.getInt("pcap.record.sort.field", 1));
 
        conf.setJobName(jobName); 
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	   
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setMapperClass(Map_Stats2.class);
        conf.setCombinerClass(Reduce_Stats2.class);
        conf.setReducerClass(Reduce_Stats2.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	} 
    
    public void startStats(Path inputDir, Path outputDir, long cap_start, long cap_end, boolean fh_skip) throws IOException {
    	
        FileSystem fs = FileSystem.get(conf);
        JobConf sGenJobconf = getStatsGenJobConf("PcapTotalStats_gen", inputDir, fh_skip);   
        sGenJobconf.setLong("pcap.file.captime.min", cap_start);
        sGenJobconf.setLong("pcap.file.captime.max", cap_end);
//      System.out.println(sGenJobconf.getLong("pcap.file.captime.min", 11));
//		System.out.println(String.format("%1$tY-%1$tm-%1$td", sGenJobconf.getLong("pcap.file.captime.min", 11)*1000));			
//		System.out.println(sGenJobconf.getInt("pcap.record.rate.interval", 0));
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(sGenJobconf))) {
          fs.delete(FileOutputFormat.getOutputPath(sGenJobconf), true);
        }
        JobClient.runJob(sGenJobconf);  

        Path sGenOutputDir = FileOutputFormat.getOutputPath(sGenJobconf);
        JobConf sReduceJobConf = getStatsReduceJobConf("PcapTotalStats_red", sGenOutputDir, outputDir);  
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(sReduceJobConf))) {
          fs.delete(FileOutputFormat.getOutputPath(sReduceJobConf), true);
        }
        JobClient.runJob(sReduceJobConf);
      }

    
    /*******************************************
				RATE function
	*******************************************/
	
	public static class Map_Rate extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, Text>{	
		int interval = 0;		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);			
		}
		
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
			
			int pkts = 1;	
			byte[] eth_type = new byte[2];
			byte[] ip_ver = {0x00};
			byte[] bcap_time = new byte[4];
			long cap_time =0;	
			byte[] bc= new byte[PcapRec.LEN_VAL1];				
			String new_key = "";
							

			byte[] value_bytes = value.getBytes();	
			if(value_bytes.length<16){
				output.collect(new Text(" non-IP"), new Text(0 +" "+pkts));
				return;
			}
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			System.arraycopy(value_bytes, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
			cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));
			cap_time = cap_time - (cap_time % interval);
			
			new_key += cap_time;
			if(BinaryUtils.byteToInt(eth_type) == PcapRec.IP_PROTO) {
				System.arraycopy(value_bytes, PcapRec.POS_IP_VER, ip_ver, 0, PcapRec.LEN_IP_VER);
				
				if((BinaryUtils.byteToInt(ip_ver) & PcapRec.IPV4) == PcapRec.IPV4){
					new_key += " IPv4";
					System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bc, 2, PcapRec.LEN_IP_BYTES);					
				}else{
					new_key += " IPv6";
					System.arraycopy(value_bytes, PcapRec.POS_IP_BYTES, bc, 2, PcapRec.LEN_IP_BYTES);					
				}
			}else{		
				new_key = " non-IP"	;
			}	
			
			output.collect(new Text(new_key), new Text(Bytes.toLong(bc)+" "+pkts));	
		}
	}
	
    public static class Reduce_Rate extends MapReduceBase 
    	implements Reducer<Text, Text, Text, Text> {
    	
		int interval = 0;		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);			
		}
		
        public void reduce(Text key, Iterator<Text> value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                        throws IOException {
 
 	       String line = null; 	  	
	       StringTokenizer stok = null;   
	       String result = "";
	       long bc =0;
	       long pc =0;
           
	       while(value.hasNext()){  
	    	   line = value.next().toString();
	    	   stok = new StringTokenizer(line);
	    	   if(line.length()<0)  continue;
		       
		       bc += Long.parseLong(stok.nextToken().trim());
		       pc += Long.parseLong(stok.nextToken().trim());
	       }	  
	       result = bc + "  " + pc + "  " + (float)bc/interval + "  " + (float)pc/interval;
	       output.collect(key, new Text(result));
        }
    }
      
	private JobConf getRateGenJobConf(String jobName, Path inFilePath, Path outFilePath){

        conf.setJobName(jobName);
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);	       
        conf.setInputFormat(PcapInputFormat.class);            
        conf.setOutputFormat(TextOutputFormat.class);  
        conf.setMapperClass(Map_Rate.class);
        conf.setCombinerClass(Reduce_Rate.class);
        conf.setReducerClass(Reduce_Rate.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}
	
    public void startRate(Path inputDir, Path outputDir, long cap_start, long cap_end){// throws IOException {
        
    	try {
    		FileSystem fs = FileSystem.get(conf);
            JobConf rGenJobconf = getRateGenJobConf("RateGeneration", inputDir, outputDir);        
            rGenJobconf.setLong("pcap.file.captime.min", cap_start);
            rGenJobconf.setLong("pcap.file.captime.max", cap_end);
            
            // delete any output that might exist from a previous run of this job
            if (fs.exists(FileOutputFormat.getOutputPath(rGenJobconf))) {
              fs.delete(FileOutputFormat.getOutputPath(rGenJobconf), true);
            }        
    		JobClient.runJob(rGenJobconf);	
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
      }

 
	/*******************************************
				FLOW GEN function
     *******************************************/

	public static class Map_FlowGen extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, BytesWritable, BytesWritable>{
		
		int interval = 0;		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);			
		}	
		
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {		
			
			
			ExtendedBytesWritable new_key = new ExtendedBytesWritable(new byte[17]);
			ExtendedBytesWritable new_value = new ExtendedBytesWritable(new byte[PcapRec.LEN_VAL3]);	
			
			byte[] pkts = {0x00, 0x01};	
			byte[] eth_type = new byte[2];
			byte[] bcap_time = new byte[4];
			long cap_time = 0;			
			byte[] value_bytes = value.getBytes();			
			if(value_bytes.length<MIN_PKT_SIZE) return;			
			
			System.arraycopy(value_bytes, PcapRec.POS_ETH_TYPE, eth_type, 0, PcapRec.LEN_ETH_TYPE);
			
			if(BinaryUtils.byteToInt(eth_type) != PcapRec.IP_PROTO) return;
			
			System.arraycopy(value_bytes, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
			cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));
			cap_time = cap_time - (cap_time % interval);
			
			new_key.set(value_bytes, PcapRec.POS_SIP, 0, 12);		
			new_key.set(value_bytes, PcapRec.POS_PT, 12, 1);			
			new_key.set(BinaryUtils.uIntToBytes(cap_time), 0, 13, 4);
			
			new_value.set(value_bytes, PcapRec.POS_IP_BYTES, PcapRec.POS_V_BC,  PcapRec.LEN_IP_BYTES);				
			new_value.set(pkts, 0, PcapRec.POS_V_PC, PcapRec.LEN_IP_BYTES);				
			
			output.collect(new BytesWritable(new_key.getBytes()), new BytesWritable(new_value.getBytes()));
		}
	}
   
    public static class Reduce_FlowGen extends MapReduceBase 
	implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
      
	    public void reduce(BytesWritable key, Iterator<BytesWritable> value,
            OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
            throws IOException {
			
	        byte[] sum = new byte[PcapRec.LEN_VAL3];
	        byte[] data = new byte[PcapRec.LEN_VAL3];  
	        byte[] flows = {0x00, 0x00, 0x00, 0x01};
	        ExtendedBytesWritable new_value = new ExtendedBytesWritable(new byte[PcapRec.LEN_VAL3]);
	  	    	
	       while(value.hasNext()){  
	    	   data = value.next().getBytes();			 				
	    	   sum = BitAdder.addBinary(sum, data, PcapRec.LEN_VAL3);
	       }      
	       System.arraycopy(flows, 0, sum, PcapRec.LEN_VAL2, PcapRec.LEN_VAL1);
	       new_value.set(sum, 0, 0, PcapRec.LEN_VAL3);	
	       
	       output.collect(key, new BytesWritable(new_value.getBytes()));  	       
	    }
    }
    
	private JobConf getFlowGenJobConf(String jobName, Path inFilePath){
		
	    Path Output = new Path(jobName);			
        conf.setJobName(jobName);     
        conf.setNumReduceTasks(10);
        
        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputValueClass(BytesWritable.class);	       
        conf.setInputFormat(PcapInputFormat.class);      
        conf.setOutputFormat(BinaryOutputFormat.class);        
        conf.setMapperClass(Map_FlowGen.class);
        conf.setCombinerClass(Reduce_FlowGen.class);          
        conf.setReducerClass(Reduce_FlowGen.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, Output);
        
        return conf;
	}
	
	
	
    /*******************************************
				FLOW RATE function
     *******************************************/
    
	public static class Map_FlowRate extends MapReduceBase 
	implements Mapper<BytesWritable, BytesWritable, LongWritable, Text>{	
		
		public void map
				(BytesWritable key, BytesWritable value, 
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {		

			byte[] new_key = new byte[4];	
			byte[] tuple = new byte[17];
			
			byte[] bc= new byte[PcapRec.LEN_VAL1];
			byte[] pc= new byte[PcapRec.LEN_VAL1];
			byte[] fc= new byte[PcapRec.LEN_VAL1];
			
			byte[] ip = new byte[4];
			byte[] port = new byte[2];
//			byte[] proto = new byte[2];
			byte[] proto = new byte[1];
			
			long ibc =0;
			long ipc =0;
			long ifc =0;
		
			byte[] value_bytes = value.getBytes();	
			
			System.arraycopy(value_bytes, 13, new_key, 0, new_key.length);				
			System.arraycopy(value_bytes, 17, bc, 0, PcapRec.LEN_VAL1);	 
			System.arraycopy(value_bytes, 17+PcapRec.LEN_VAL1, pc, 0, PcapRec.LEN_VAL1);
			System.arraycopy(value_bytes, 17+PcapRec.LEN_VAL2, fc, 0, PcapRec.LEN_VAL1);
 
			long cap_time = Bytes.toLong(new_key);
			String strTuple = cap_time + " " ;
			System.arraycopy(value_bytes, 0, ip, 0, 4);
			strTuple += CommonData.longTostrIp(Bytes.toLong(ip))+" ";
			System.arraycopy(value_bytes, 4, ip, 0, 4);
			strTuple += CommonData.longTostrIp(Bytes.toLong(ip))+" ";
			System.arraycopy(value_bytes, 8, port, 0, 2);
			strTuple += Bytes.toLong(port)+" ";		
			System.arraycopy(value_bytes, 10, port, 0, 2);
			strTuple += Bytes.toInt(port)+" ";	
			System.arraycopy(value_bytes, 12, proto, 0, 1);
			strTuple += Bytes.toInt(proto)+" ";	
			
			ibc = Bytes.toLong(bc);
			ipc = Bytes.toLong(pc);
			ifc = Bytes.toLong(fc);
			
			/* 5 tuples */
			System.arraycopy(value_bytes, 0, tuple, 0, tuple.length);				
			String result = tuple + " "+ ibc + " " + ipc + " " + ifc ;	
			output.collect(new LongWritable(cap_time), new Text(strTuple +":"+ result));
		}
	}

    public static class Reduce_FlowRate extends MapReduceBase 
	implements Reducer<LongWritable, Text, LongWritable, Text> {	
    	
		int interval = 0;		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 60);			
		}
		
	    public void reduce(LongWritable key, Iterator<Text> value,
	                    OutputCollector<LongWritable, Text> output, Reporter reporter)
	                    throws IOException {
	    	
	        long bc =0;
	        long pc =0;
	        long fc =0;      
	        int ec = 0;
	        
	        String result ="";
	        String prev ="";
	        String line = null;
	        StringTokenizer stok = null;
	        StringTokenizer subtok = null;
	        String strTuple = "";
	        String f_tuple = "";
	       
	       while(value.hasNext()){  
	    	   line = value.next().toString();
	    	   stok = new StringTokenizer(line,":");
	    	   if(line.length()<0)  continue;
	    	   strTuple = stok.nextToken();
	    	   
	    	   subtok = new StringTokenizer(stok.nextToken());
	    	   f_tuple = subtok.nextToken();
	    	   
		       if(!f_tuple.equals(prev)){
		    	   ec++;
		    	   prev = f_tuple;
		       }		       
		       bc += Long.parseLong(subtok.nextToken().trim());
		       pc += Long.parseLong(subtok.nextToken().trim());
		       fc += Long.parseLong(subtok.nextToken().trim());
	       }	  
	       result = strTuple + " | " + bc + "  " + pc + "  " + fc + "  " +  ec + "  " + (float)bc/interval + "  " + (float)pc/interval + "  " + (float)fc/interval;
	       output.collect(key, new Text(result));                   
	    }
    }
  	
	private JobConf getFlowStatsJobConf(String jobName, Path inFilePath, Path outFilePath){
				
		JobConf conf = new JobConf(P3CoralProgram.class);
		
        conf.addResource("p3-default.xml");
        conf.setInt("pcap.record.rate.interval", this.conf.getInt("pcap.record.rate.interval", 60));
        conf.setInt("pcap.record.key.pos", this.conf.getInt("pcap.record.key.pos", PcapRec.POS_SIP));
        conf.setInt("pcap.record.key.len", this.conf.getInt("pcap.record.key.len", PcapRec.LEN_IPADDR));
        conf.setInt("pcap.record.sort.field", this.conf.getInt("pcap.record.sort.field", 1));
 
        conf.setJobName(jobName);
		conf.setInt("io.file.buffer.size", FLOW_RECORD_SIZE); 
        conf.setNumReduceTasks(10);
        
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);	 
        conf.setInputFormat(BinaryInputFormat.class);         
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setMapperClass(Map_FlowRate.class);
        conf.setReducerClass(Reduce_FlowRate.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}


	
    public void startFlowStats(Path inputDir, Path outputDir,long cap_start, long cap_end, boolean fh_skip) throws IOException {
    	
        FileSystem fs = FileSystem.get(conf);
        JobConf fGenJobconf = getFlowGenJobConf("PcapPeriodicFlowStats_gen", inputDir); 
        fGenJobconf.setLong("pcap.file.captime.min", cap_start);
        fGenJobconf.setLong("pcap.file.captime.max", cap_end);
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(fGenJobconf))) {
          fs.delete(FileOutputFormat.getOutputPath(fGenJobconf), true);
        }
        JobClient.runJob(fGenJobconf);  

        Path fGenOutputDir = FileOutputFormat.getOutputPath(fGenJobconf);
        JobConf fReduceJobConf = getFlowStatsJobConf("PcapPeriodicFlowStats_red", fGenOutputDir, outputDir);
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(fReduceJobConf))) {
          fs.delete(FileOutputFormat.getOutputPath(fReduceJobConf), true);
        }
        JobClient.runJob(fReduceJobConf);  
    }

    /*******************************************
			TOP N function
	*******************************************/
    
	public static class Map_TopN extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		
		int key_field = 0;		
		public void configure(JobConf conf){
			key_field = conf.getInt("pcap.record.sort.field", 1);		
		}
	
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
			
			String line = value.toString(); 	  	
			String tokens[] = line.split(" ");
			String new_key = tokens[key_field];
			
			output.collect(new Text(new_key), value);	
		}
	}
	
    public static class Reduce_TopN extends MapReduceBase 
    	implements Reducer<Text, Text, Text, Text> {
    	
    	long topN = 0;
		public void configure(JobConf conf){
			topN = conf.getLong("pcap.record.sort.topN", 4294967295L);		
		}
		
        public void reduce(Text key, Iterator<Text> value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                        throws IOException {
        	
           int cnt = 0;				          
	       while(value.hasNext()){  
	           if (cnt<topN){
	    	       output.collect(key, value.next());	  
	    	       cnt++;
	           }
	       }	  
        }
    }      

	private JobConf getTopNJobConf(String jobName, Path inFilePath, Path outFilePath){
		
		JobConf conf = new JobConf(P3CoralProgram.class);
		
        conf.addResource("p3-default.xml");
        conf.setInt("pcap.record.rate.interval", this.conf.getInt("pcap.record.rate.interval", 60));
        conf.setInt("pcap.record.key.pos", this.conf.getInt("pcap.record.key.pos", PcapRec.POS_SIP));
        conf.setInt("pcap.record.key.len", this.conf.getInt("pcap.record.key.len", PcapRec.LEN_IPADDR));
        conf.setInt("pcap.record.sort.field", this.conf.getInt("pcap.record.sort.field", 1));
 
        conf.setJobName(jobName); 
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);	       
        conf.setInputFormat(TextInputFormat.class);            
        conf.setOutputFormat(TextOutputFormat.class);  
        conf.setMapperClass(Map_TopN.class);
        conf.setCombinerClass(Reduce_TopN.class);
        conf.setReducerClass(Reduce_TopN.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}   
	
    /*******************************************
			for Test
    *******************************************/
	public static class PcapMapper extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable>{
		public void map
				(LongWritable key, BytesWritable value, 
				OutputCollector<LongWritable, BytesWritable> output, Reporter reporter) throws IOException {				
			output.collect(key,value);
		}
	}
	
	private JobConf getTestConf(String jobName, Path inFilePath, Path outFilePath){
		
	    Path Output = new Path(jobName);			
        conf.setJobName(jobName);     
       
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(BytesWritable.class);	       
        conf.setInputFormat(PcapInputFormat.class);        
        conf.setOutputFormat(TextOutputFormat.class);        
        conf.setMapperClass(PcapMapper.class);
        conf.setPartitionerClass(HashPartitioner.class);

 //       conf.setCombinerClass(Reduce_CountUp.class);          
 //       conf.setReducerClass(Reduce_CountUp.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, Output);
        
        return conf;
	}
    
    public void startTest(Path inputDir, Path outputDir, long cap_start, long cap_end){// throws IOException {
        
	try {
		
		FileSystem fs = FileSystem.get(conf);
        JobConf myconf = getTestConf("Test", inputDir, outputDir);        
        
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(myconf))) {
          fs.delete(FileOutputFormat.getOutputPath(myconf), true);
        }        
		JobClient.runJob(myconf);	
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

}
