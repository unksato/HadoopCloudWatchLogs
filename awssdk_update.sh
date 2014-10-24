#!/bin/bash
# AWS SDK update shell for AMI 3.2.1

rm /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/aws-java-sdk-1.7.8.jar

wget -t 5 -O /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/aws-java-sdk-core-1.8.10.2.jar http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.8.10.2/aws-java-sdk-core-1.8.10.2.jar
wget -t 5 -O /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/aws-java-sdk-1.8.10.2.jar http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.8.10.2/aws-java-sdk-1.8.10.2.jar

chmod 755 /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/aws-java-sdk-core-1.8.10.2.jar
chmod 755 /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/aws-java-sdk-1.8.10.2.jar
