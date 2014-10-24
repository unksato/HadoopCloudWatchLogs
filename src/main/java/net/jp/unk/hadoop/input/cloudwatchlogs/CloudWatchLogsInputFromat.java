package net.jp.unk.hadoop.input.cloudwatchlogs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.jp.unk.hadoop.aws.util.CloudWatchLogsWrapper;
import net.jp.unk.hadoop.input.cloudwatchlogs.hive.exception.CloudWatchLogsHiveException;
import net.jp.unk.hadoop.input.cloudwatchlogs.names.CloudWatchLogsKeyNames;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.amazonaws.services.logs.model.LogStream;


public class CloudWatchLogsInputFromat 
extends org.apache.hadoop.mapreduce.InputFormat<LongWritable,Text> 
implements org.apache.hadoop.mapred.InputFormat<LongWritable,Text> {

	private final static String DEFAULT_DATE_FORMAT = "yyyy/MM/dd hh:mm:ss";
	
	private final static long ONE_DAY = 24 * 60 * 60 * 1000;
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();

		List<CloudWatchLogsInputSplit> splits;
		try {
			splits = getSplits(conf);
		} catch (CloudWatchLogsHiveException e) {
			throw new IOException(e);
		}
		
		List<InputSplit> resultSplits = new ArrayList<InputSplit>(splits.size());
		
		for(InputSplit split : splits){
			resultSplits.add(split);
		}		
        
		return resultSplits;
	}

	private List<CloudWatchLogsInputSplit> getSplits(Configuration conf) throws CloudWatchLogsHiveException{
		String logGroup = conf.get(CloudWatchLogsKeyNames.LOG_GROUP);
        String logStreamStr = conf.get(CloudWatchLogsKeyNames.STREAM_NAMES);
        
        String[] logStreams = null;

        CloudWatchLogsWrapper client = new CloudWatchLogsWrapper(conf);
        
        if(logStreamStr == null || !logStreamStr.isEmpty()){
        	 List<LogStream> streams = client.getLogStreams();
        	 
        	 logStreams = new String[streams.size()];
        	 int i = 0;
        	 for(LogStream stream : streams){
        		 logStreams[i] = stream.getLogStreamName();
        		 i++;
        	 }
        }else{
        	logStreams = logStreamStr.split(",");
        }
        
        String startTimeStr = conf.get(CloudWatchLogsKeyNames.START_TIME);
        String endTimeStr = conf.get(CloudWatchLogsKeyNames.END_TIME);
        
        String dateTimeFormat = conf.get(CloudWatchLogsKeyNames.DATE_FORMAT);
        
        long indicatedStartTime = stringToUnixTime(startTimeStr,dateTimeFormat);
        long indicatedEndTime = stringToUnixTime(endTimeStr,dateTimeFormat);
    	if(indicatedEndTime == 0){
    		indicatedEndTime = System.currentTimeMillis();;
    	}
		
        List<CloudWatchLogsInputSplit> splits = new ArrayList<CloudWatchLogsInputSplit>();
        
        for(String stream : logStreams){
        	
        	stream = stream.trim();  
        		
        	long startTime = indicatedStartTime;
        	
        	if(startTime == 0){
        		startTime = client.getStreamHeadIngestionTime(stream);
        	}
        	
        	boolean isEnd = false;
        	
        	while(!isEnd){
        		long endTime = startTime + ONE_DAY;
        		if(endTime > indicatedEndTime){
        			endTime = indicatedEndTime;
        			isEnd = true;
        		}

        		if(startTime >= endTime){
        			break;
        		}
        		
        		CloudWatchLogsInputSplit split = new CloudWatchLogsInputSplit(stream,logGroup,startTime,endTime);
            	splits.add(split);
            	startTime = endTime + 1;
        	}
        }
        
		return splits;
	}
	
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new CloudWatchLogsReader();
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<LongWritable, Text> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf jobConf,
			Reporter reporter) throws IOException {
		
		return new CloudWatchLogsReader((CloudWatchLogsInputSplit)split, jobConf, reporter);

	}

	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {

		List<CloudWatchLogsInputSplit> splits;
		try {
			splits = getSplits(jobConf);
		} catch (CloudWatchLogsHiveException e) {
			throw new IOException(e);
		}
		return (org.apache.hadoop.mapred.InputSplit[])splits.toArray(new org.apache.hadoop.mapred.InputSplit[splits.size()]);
	}
	
	private static long stringToUnixTime(String dateStr, String formatStr) throws CloudWatchLogsHiveException{

		if(dateStr == null || dateStr.isEmpty()){
			return 0;
		}
	
		if(formatStr == null){
			formatStr = DEFAULT_DATE_FORMAT;
		}
		
		SimpleDateFormat format = new SimpleDateFormat(formatStr);
		
		long unixtime = 0;
		
		try{
			Date date = format.parse(dateStr);
			unixtime = date.getTime();
		}catch(ParseException e){
			throw new CloudWatchLogsHiveException("date format is \"" + formatStr + "\"",e);
		}
		return unixtime;
	}
	
}
