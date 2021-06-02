// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

name := "SparkKinesisExample"
version := "1.1"
scalaVersion := "2.12.10"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.1.1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.977"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kinesis-asl" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "software.amazon.kinesis" % "amazon-kinesis-client" % "2.2.10"
