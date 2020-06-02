#!/bin/bash

APPNAME=$1
STREAMNAME=$2

REGION=`curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
CP="/home/hadoop"
export PATH=$PATH:$CP/sbt/bin
( cd $CP/aws-cloudwatch-metrics-custom-spark-listener && sbt "runMain ExampleKinesisProducer $STREAMNAME" )

