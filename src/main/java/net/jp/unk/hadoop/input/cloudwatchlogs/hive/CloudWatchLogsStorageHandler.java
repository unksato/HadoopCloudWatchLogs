package net.jp.unk.hadoop.input.cloudwatchlogs.hive;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

@SuppressWarnings("deprecation")
public class CloudWatchLogsStorageHandler implements HiveStorageHandler{

	private Configuration conf;
	
	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		configureTableJobProperties(tableDesc, jobProperties);
	}

	@Override
	public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		configureTableJobProperties(tableDesc, jobProperties);
	}

	@Override
	@Deprecated
	public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		Properties p = tableDesc.getProperties();
		Enumeration<Object> keys = p.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = p.getProperty(key);
			jobProperties.put(key, value);
		}
	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		return new DefaultHiveAuthorizationProvider();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return CloudWatchLogsHiveInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return HiveSequenceFileOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return LazySimpleSerDe.class;
	}
}
