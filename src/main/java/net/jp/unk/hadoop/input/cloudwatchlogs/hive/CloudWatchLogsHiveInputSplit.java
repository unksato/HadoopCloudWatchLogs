package net.jp.unk.hadoop.input.cloudwatchlogs.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jp.unk.hadoop.input.cloudwatchlogs.CloudWatchLogsInputSplit;

public class CloudWatchLogsHiveInputSplit extends FileSplit implements InputSplit{

	private CloudWatchLogsInputSplit split;
	static Logger log = LoggerFactory.getLogger(CloudWatchLogsHiveInputSplit.class);
	
	public CloudWatchLogsHiveInputSplit(){
		super((Path) null ,0,0,(String[])null);
		this.split = new CloudWatchLogsInputSplit();
	};
	
	public CloudWatchLogsHiveInputSplit(String stream, String logGroup, long startTime, long endTime){
		this(null,new CloudWatchLogsInputSplit(stream, logGroup, startTime, endTime));
	}
	
	public CloudWatchLogsHiveInputSplit(Path tablePath, CloudWatchLogsInputSplit split ) {
		super(tablePath,0,0,(String[])null);
		this.split = split;
	}
	
	@Override
	public long getLength(){
		return this.split.getLength();
	}

	@Override
	public String[] getLocations(){
		return this.split.getLocations();
	}
	
	@Override
	public void write(DataOutput out) throws IOException{
		super.write(out);
		this.split.write(out);
	}
	
	public void readFields(DataInput in) throws IOException{
		super.readFields(in);
		this.split.readFields(in);
	}
	
	public CloudWatchLogsInputSplit getCloudWatchLogsInputSplit(){
		return this.split;
	}

}
