// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

name := "SparkKinesisExample"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.659"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "software.amazon.kinesis" % "amazon-kinesis-client" % "2.2.10"
