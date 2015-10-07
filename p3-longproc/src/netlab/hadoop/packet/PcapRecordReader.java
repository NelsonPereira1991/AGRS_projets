package netlab.hadoop.packet;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


import p3.hadoop.common.pcap.lib.PcapRec;
import p3.common.lib.BinaryUtils;
import p3.common.lib.Bytes;


public class PcapRecordReader implements RecordReader<LongWritable, BytesWritable> {


	DataInputStream stream = null;
	FSDataInputStream baseStream = null;

	long k;

	long start = 0L;
	long end;


	Reporter reporter;

	public PcapRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws java.io.IOException {

		FileSplit fileSplit = (FileSplit)split;
		if (fileSplit == null) return;

		start = fileSplit.getStart();
		end = start + fileSplit.getLength();

//		System.out.println("split length = " + (long)(end-start));

		Path path = fileSplit.getPath();
		if (path == null) return;

		FileSystem fs = path.getFileSystem(jobConf);
		if (fs == null) return;

		baseStream = fs.open(path);
		stream = baseStream;

		if (baseStream != null) 
		{
			baseStream.seek(start);
		}

		this.reporter = reporter;
		k = 0;
	}


	@Override
	public void close() throws IOException {
		if (stream != null) stream.close();
	}

	public boolean next(LongWritable key, BytesWritable value) throws IOException 
	{
		if (stream == null) return false;

		byte[] bcap_time = new byte[4];

		boolean foundPacket = false;

		while (!foundPacket)
		{

			if (getPos() + 16 > end) 
			{
				return false;
			}

			long pos = getPos();
		
			byte[] pcapheader = read(16);

			if (pcapheader == null) return false;

			System.arraycopy(pcapheader, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
			Long cap_time = new Long(0);			
			cap_time = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

			System.arraycopy(pcapheader, PcapRec.POS_TSTMP+4, bcap_time, 0, 4);				
			Long cap_time_2 = new Long(0);		
			cap_time_2 = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

			System.arraycopy(pcapheader, PcapRec.POS_TSTMP+8, bcap_time, 0, 4);	
			Long cap_len = new Long(0);			
			cap_len = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

			System.arraycopy(pcapheader, PcapRec.POS_TSTMP+12, bcap_time, 0, 4);	
			Long wire_len = new Long(0);			
			wire_len = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));


			foundPacket = checkPcapHeader_1 (cap_time, cap_time_2, cap_len, wire_len);

			if (foundPacket)
			{
			
				baseStream.seek(pos + cap_len);
				byte[] pcapheader2 = read(16);

				if (pcapheader2 != null) 
				{
					System.arraycopy(pcapheader2, PcapRec.POS_TSTMP, bcap_time, 0, 4);	
					Long cap_time_pac2 = new Long(0);			
					cap_time_pac2 = Bytes.toLong(BinaryUtils.flipBO(bcap_time,4));

					if (cap_time_pac2 - cap_time < 0 && cap_time_pac2 - cap_time > 3600*24*30) foundPacket = false;
				}
			}
				
			if (foundPacket) 
			{
				baseStream.seek(pos);
				long packet_length = cap_len+16;
				byte[] pcap = read((int) packet_length);
				if (pcap == null) 
				{
					return false;
				}
				else {
					value.set(pcap, 0, (int) packet_length);
					key.set(k);
					k++;

					reporter.setStatus("Read " + (long) (getPos()-start) + " of " + (long)(end-start) + " bytes");
					reporter.progress();

					return true;
				}
					
			}
			else
			{
				baseStream.seek (pos + 1);
			}
		}

		return false;
	}

	public boolean checkPcapHeader_1(Long cap_time, Long cap_time_2, Long cap_len, Long wire_len)
	{
		if (cap_len > 10000) return false;

		if (wire_len > 10000) return false;

		if (wire_len < cap_len) return false;

		return true; 
	}


	public byte[] read(int howmany) throws IOException
	{
		byte[] b = new byte[howmany];
		int c = 0;
		int t = 0;
		while (c < howmany && t >= 0)
		{
			int lefttoread = howmany - c;
			t = stream.read(b, c, lefttoread);
			if (t >= 0) 
			{
				c = c + t;
			}
		}

		if (t < 0) return null;
		return b;
	}




	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		if (baseStream != null) 
		{
			return baseStream.getPos();
		}
		return 0;
	}


	@Override
	public float getProgress() throws IOException {
		if (start == end) return 0;
		return Math.min(1.0f, (getPos() - start) / (float)(end - start));
	}

}
