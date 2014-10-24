package net.jp.unk.hadoop.input.cloudwatchlogs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudWatchLogsInputSplit extends org.apache.hadoop.mapreduce.InputSplit implements org.apache.hadoop.mapred.InputSplit{

	static Logger log = LoggerFactory.getLogger(CloudWatchLogsInputSplit.class);
	
	private String stream;
	private String logGroup;
	private long startTime;
	private long endTime;
	
	public CloudWatchLogsInputSplit(){};
	
	public CloudWatchLogsInputSplit(String stream, String logGroup, long startTime, long endTime){
		this.stream = stream;
		this.logGroup = logGroup;
		this.startTime = startTime;
		this.endTime = endTime;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		this.stream = input.readUTF();
		this.logGroup = input.readUTF();
		this.startTime = input.readLong();
		this.endTime = input.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.stream);
		out.writeUTF(this.logGroup);
		out.writeLong(this.startTime);
		out.writeLong(this.endTime);
	}

	public String getStream(){
		return this.stream;
	}
	
	public String getLogGroup(){
		return this.logGroup;
	}
	
	public long getStartTime(){
		return this.startTime;
	}
	
	public long getEndTime(){
		return this.endTime;
	}
	
	@Override
	public long getLength() {
		return 0;
	}

	@Override
	public String[] getLocations() {
		return new String[0];
	}

}
