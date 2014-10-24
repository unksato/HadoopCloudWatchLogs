package net.jp.unk.hadoop.aws.util;

import java.lang.reflect.Constructor;

import net.jp.unk.hadoop.input.cloudwatchlogs.names.CloudWatchLogsKeyNames;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.logs.AWSLogsClient;

public class AWSUtil {
	public static Regions stringToRegion(String regionStr){
		
		if(regionStr == null || regionStr.isEmpty()){
			return null;
		}
		
		String name = regionStr.replace("-", "_").toUpperCase();
		
		return Regions.valueOf(name);
	}
	
	public static AWSLogsClient createCloudWatchLogsClient(Configuration conf){
		return 	createClient(AWSLogsClient.class,
				conf.get(CloudWatchLogsKeyNames.AWS_ACCESSKEY),
				conf.get(CloudWatchLogsKeyNames.AWS_SECRETKEY),
				conf.get(CloudWatchLogsKeyNames.AWS_REGION),
				conf.get(CloudWatchLogsKeyNames.AWS_ENDPOINT)
				);
	}
	
	public static <T> T createClient(Class<? extends AmazonWebServiceClient> cls, String accessKey, String secretKey, String region, String endpoint){

		AWSCredentials credentials = null;
		
		if(StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)){
			return createClient(cls,region,endpoint);
		}else{
    		credentials = new BasicAWSCredentials(accessKey,secretKey);
    	}
    	
		return createClient(cls, credentials, region, endpoint);
	}
	
	public static <T> T createClient(Class<? extends AmazonWebServiceClient> cls, String region, String endpoint){
		AWSCredentials credentials = new InstanceProfileCredentialsProvider().getCredentials();
		return createClient(cls, credentials, region, endpoint);
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T createClient(Class<? extends AmazonWebServiceClient> cls,  AWSCredentials credentials, String region, String endpoint){
		Class<?>[] types = { AWSCredentials.class };
		Constructor<?> constructor;
		
		try {
			constructor = (Constructor<?>) cls.getConstructor(types);
		} catch (Exception e) {
			return null;
		}
		
		Object[] args = { credentials };
		
		AmazonWebServiceClient clinet;
		try {
			clinet = (AmazonWebServiceClient) constructor.newInstance(args);
		} catch (Exception e){
			return null;
		}

		Regions regions = stringToRegion(region);
    
		if(regions != null){
			Region rg = Region.getRegion(regions);
			clinet.setRegion(rg);
		}
		
        if(endpoint != null && !endpoint.isEmpty()){
        	clinet.setEndpoint(endpoint);
        }
		
		return (T)clinet;
		
	}
}
