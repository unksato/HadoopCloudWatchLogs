CloudWatchLogs Library for Hadoop/Hive
============================================

Hive CloudWatch Logs Storage Handler include CloudWatch Logs 

# How to build

    mvn package

# How to use (Hive)
## Deploy
```
hive> add jar /home/hadoop/hadoop-cloudwatchlogs-0.0.1-SNAPSHOT.jar;
```

## Create Table
```
CREATE TABLE cloudwatchlogs (
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referrer STRING,
  agent STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") ([0-9]*) ([0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")"
)
STORED BY
'net.jp.unk.hadoop.input.cloudwatchlogs.hive.CloudWatchLogsStorageHandler'
TBLPROPERTIES("cloudwatchlogs.accesskey"="AWS_ACCESSKEY",
              "cloudwatchlogs.secretkey"="AWS_SECRETKEY",
              "cloudwatchlogs.loggroup"="CLOUDWATCHLOGS_LOGGROUP",
              "cloudwatchlogs.streams"="CLOUDWATCHLOGS_STREAM",
              "cloudwatchlogs.starttime"="2014/09/23 00:00:00",
              "cloudwatchlogs.endtime"="2014/09/23 23:59:59");
```

## Table properties

* cloudwatchlogs.accesskey (optional)

* cloudwatchlogs.secretkey (optional)

* cloudwatchlogs.loggroup (mandatory)

* cloudwatchlogs.streams (optional)

* cloudwatchlogs.datetime.format (optional)

* cloudwatchlogs.starttime (optional)

* cloudwatchlogs.endtime (optional)
