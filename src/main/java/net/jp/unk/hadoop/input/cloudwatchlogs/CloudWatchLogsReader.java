package net.jp.unk.hadoop.input.cloudwatchlogs;

import java.io.IOException;

import net.jp.unk.hadoop.aws.util.CloudWatchLogsWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.logs.model.OutputLogEvent;

public class CloudWatchLogsReader 
extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text>
implements  org.apache.hadoop.mapred.RecordReader<LongWritable, Text>{

	
	static Logger log = LoggerFactory.getLogger(CloudWatchLogsReader.class);
	
	private CloudWatchLogsInputSplit split = null;
	private CloudWatchLogsWrapper client;
	
	private LongWritable currentKey = null;
	private Text currentValue = null;

	public CloudWatchLogsReader(CloudWatchLogsInputSplit split, JobConf conf, Reporter reporter) throws IOException{
		initialize(split, conf);
	}
	
	@Override
	public void close() throws IOException {
		if(client != null){
			client.close();
			client = null;
		}
	}
	
	public CloudWatchLogsReader(){}

	@Override
	public float getProgress() {
		return 0;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException {
		
		initialize(split,context.getConfiguration());
	}

	
	private void initialize(InputSplit split, Configuration conf)
				throws IOException {
		this.split = (CloudWatchLogsInputSplit) split;
		
		client = new CloudWatchLogsWrapper(conf,this.split.getStream(),this.split.getStartTime(),this.split.getEndTime());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (currentKey == null) {
			currentKey = createKey();
        }
        if (currentValue == null) {
        	currentValue = createValue();
        }
        
        return next(this.currentKey,this.currentValue);
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		OutputLogEvent event = client.getNextLogEvent();
		
		if(event != null){
			key.set(event.getTimestamp());
			value.set(event.getMessage());
			
			return true;
		}
		
		return false;
	}
}
