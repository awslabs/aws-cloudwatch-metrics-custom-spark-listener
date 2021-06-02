#!/bin/bash
APPNAME=$1
STREAMNAME=$2
REGION=`curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
ISMASTER=`cat /emr/instance-controller/lib/info/instance.json | jq .isMaster`
if [ "$ISMASTER"=="true" ]; then 
 sudo yum -y install git
 CP="/home/hadoop"
 SNH="$CP/streaming-example/"
 BASE="$CP/aws-cloudwatch-metrics-custom-spark-listener"
 git clone "https://github.com/awslabs/aws-cloudwatch-metrics-custom-spark-listener.git" $BASE
 mkdir $SNH
 BASE="$CP/aws-cloudwatch-metrics-custom-spark-listener"
 CFLOC="$BASE/CloudFormation"
 wget "https://github.com/awslabs/aws-cloudwatch-metrics-custom-spark-listener/releases/download/beta-0.1/sparkkinesisexample_2.11-1.0.jar" -O $SNH/sparkkinesisexample_2.11-1.0.jar
 cp $BASE/CloudFormation/*.sh $SNH
 chmod 755 $SNH/*.sh
fi
