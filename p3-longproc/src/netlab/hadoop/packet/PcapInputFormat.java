package netlab.hadoop.packet;


import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.Path;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConfigurable;


import netlab.hadoop.packet.PcapRecordReader;



public class PcapInputFormat extends FileInputFormat<LongWritable, BytesWritable> implements JobConfigurable{


	PcapRecordReader reader = null;

	public PcapInputFormat ()
	{
	}

	@Override
  	public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws java.io.IOException
	{
		if (reader == null ) 
		{
			reader = new PcapRecordReader(split, jobConf, reporter);
		}
		return reader;
	}

	@Override
  	public void configure(JobConf jobConf)
	{
	}


	@Override
  	protected boolean isSplitable(FileSystem fs, Path path)
	{
		return true;
	}
}
