package net.jp.unk.hadoop.input.cloudwatchlogs.hive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import net.jp.unk.hadoop.input.cloudwatchlogs.CloudWatchLogsInputFromat;
import net.jp.unk.hadoop.input.cloudwatchlogs.CloudWatchLogsInputSplit;

public class CloudWatchLogsHiveInputFormat extends CloudWatchLogsInputFromat{

	public FileSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException{
		
		Path tablePath = null;
		Path[] paths = FileInputFormat.getInputPaths(jobConf);
		
		if ((paths != null) && (paths.length > 0)) {
			tablePath = paths[0];
		}

		InputSplit[] splits = super.getSplits(jobConf, numSplits);
		
		CloudWatchLogsHiveInputSplit[] resultingSplits = new CloudWatchLogsHiveInputSplit[splits.length];
		
		for(int i = 0; i < splits.length ; i++){
			resultingSplits[i] = new CloudWatchLogsHiveInputSplit(tablePath, (CloudWatchLogsInputSplit)splits[i]);
		}
		
		return resultingSplits;
	}
			
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException{
		CloudWatchLogsHiveInputSplit s = (CloudWatchLogsHiveInputSplit) split;
		return super.getRecordReader(s.getCloudWatchLogsInputSplit(), jobConf, reporter);
	}
	
}
