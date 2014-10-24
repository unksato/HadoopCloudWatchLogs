package net.jp.unk.hadoop.aws.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.jp.unk.hadoop.input.cloudwatchlogs.names.CloudWatchLogsKeyNames;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.OutputLogEvent;

public class CloudWatchLogsWrapper {

	private AWSLogsClient client;
	private String groupName;
	
	private GetLogEventsRequest logEventRequest;
	private GetLogEventsResult logEventsResult = null;
	private Iterator<OutputLogEvent> logEventIterator = null;
	
	public CloudWatchLogsWrapper(Configuration conf){
		client = AWSUtil.createCloudWatchLogsClient(conf);
		groupName = conf.get(CloudWatchLogsKeyNames.LOG_GROUP);
	}
	
	public CloudWatchLogsWrapper(Configuration conf, String stream, long startTime, long endTime){
		client = AWSUtil.createCloudWatchLogsClient(conf);
		groupName = conf.get(CloudWatchLogsKeyNames.LOG_GROUP);
		createLogEventRequest(stream, startTime, endTime);
	}
	
	public void close(){
		client.shutdown();
	}
	
	public List<LogStream> getLogStreams(){
		return getLogStreams(groupName);
	}
	
	public List<LogStream> getLogStreams(String logGroupName){
		if(logGroupName == null || logGroupName.isEmpty()){
			return null;
		}
		DescribeLogStreamsRequest req = new DescribeLogStreamsRequest(logGroupName);
		DescribeLogStreamsResult ret = client.describeLogStreams(req);
		return ret.getLogStreams();
	}

	public long getStreamHeadIngestionTime(String stream){
		GetLogEventsRequest request = new GetLogEventsRequest(groupName, stream);
		request.setStartFromHead(true);
		request.setLimit(1);
		GetLogEventsResult result = client.getLogEvents(request);

		if(result == null || result.getEvents() == null || result.getEvents().size() == 0){
			return 0L;
		}
		
		return result.getEvents().get(0).getIngestionTime() != null ? result.getEvents().get(0).getIngestionTime() : 0L;
	}
	
	public Map<String,Long> getStreamHeadIngestionTime(List<String> streams){
		Map<String,Long> results = new HashMap<String,Long>();
		
		for(String stream : streams){
			results.put(stream, getStreamHeadIngestionTime(stream));
		}
		
		return results;
	}
	
	public GetLogEventsRequest createLogEventRequest(String stream, long startTime, long endTime){
		logEventRequest = new GetLogEventsRequest(groupName,stream);
		
		if(startTime != 0){
			logEventRequest.setStartTime(startTime);
		}
		if(endTime != 0){
			logEventRequest.setEndTime(endTime);
		}
		
		return logEventRequest;
	}
	
	public OutputLogEvent getNextLogEvent(){
		if(this.logEventsResult == null){
			this.logEventsResult = client.getLogEvents(logEventRequest);
			List<OutputLogEvent> events = logEventsResult.getEvents();
			
			if(events != null){
				this.logEventIterator = events.iterator();
			}
		}
		
		if(this.logEventIterator != null && this.logEventIterator.hasNext()){
			OutputLogEvent event = logEventIterator.next();
			
			if(!logEventIterator.hasNext()){
				this.logEventRequest.setNextToken(logEventsResult.getNextBackwardToken());
				this.logEventsResult = null;
				this.logEventIterator = null;
			}
			
			return event;
		}
		
		return null;
	}
	
	
}
