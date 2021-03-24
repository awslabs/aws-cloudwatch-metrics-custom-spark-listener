#!/bin/bash
APPNAME=$1
STREAMNAME=$2
REGION=`curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
ISMASTER=`cat /emr/instance-controller/lib/info/instance.json | jq .isMaster`
if [ "$ISMASTER"=="true" ]; then 
 sudo yum -y install git
 wget "https://github.com/sbt/sbt/releases/download/v1.3.8/sbt-1.3.8.zip"
 CP="/home/hadoop"
 SNH="$CP/streaming-example/"
 tar -xzf ./sbt-1.3.8.tgz -C $CP
 export PATH=$PATH:$CP/sbt/bin
 BASE="$CP/aws-cloudwatch-metrics-custom-spark-listener"
 git clone "https://github.com/awslabs/aws-cloudwatch-metrics-custom-spark-listener.git" $BASE
 mkdir $SNH
 BASE="$CP/aws-cloudwatch-metrics-custom-spark-listener"
 CFLOC="$BASE/CloudFormation"
 ( cd $BASE && sbt package ) 
 cp $BASE/target/scala-2.11/*.jar $SNH 
 cp $BASE/CloudFormation/*.sh $SNH
 chmod 755 $SNH/*.sh
fi
