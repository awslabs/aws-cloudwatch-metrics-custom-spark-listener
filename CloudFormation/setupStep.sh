#!/bin/bash
APPNAME=$1
STREAMNAME=$2
REGION=`curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
ISMASTER=`cat /emr/instance-controller/lib/info/instance.json | jq .isMaster`
if [ "$ISMASTER"=="true" ]; then 
 curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
 sudo mv sbt-rpm.repo /etc/yum.repos.d/
 sudo yum -y install sbt git
 CP="/home/hadoop"
 SNH="$CP/streaming-example/"
 BASE="$CP/aws-cloudwatch-metrics-custom-spark-listener"
 git clone "https://github.com/awslabs/aws-cloudwatch-metrics-custom-spark-listener.git" $BASE
 mkdir $SNH
 BASE="$CP/aws-cloudwatch-metrics-custom-spark-listener"
 CFLOC="$BASE/CloudFormation"
 wget "https://github.com/awslabs/aws-cloudwatch-metrics-custom-spark-listener/releases/download/beta-0.2/sparkkinesisexample_2.12-0.2.jar" -O $SNH/sparkkinesisexample_2.12-0.2.jar
 cp $BASE/CloudFormation/*.sh $SNH
 chmod 755 $SNH/*.sh
fi
