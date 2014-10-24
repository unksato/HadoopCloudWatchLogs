package net.jp.unk.hadoop.input.cloudwatchlogs.hive.exception;

public class CloudWatchLogsHiveException extends Exception{

	private static final long serialVersionUID = 1L;

	public CloudWatchLogsHiveException(String message){
		super(message);
	}
	
	public CloudWatchLogsHiveException(String message, Throwable e){
		super(message,e);
	}
	
}
