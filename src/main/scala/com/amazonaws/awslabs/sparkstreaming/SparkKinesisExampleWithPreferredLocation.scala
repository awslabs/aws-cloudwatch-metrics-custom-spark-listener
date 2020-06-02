// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.awslabs.sparkstreaming

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Milliseconds}

import org.apache.spark.streaming.kinesis.CustomKinesisInputDStream

import com.amazonaws.awslabs.sparkstreaming.listener._
import com.amazonaws.awslabs.utils._

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.NodeState

import collection.JavaConverters._

object SparkKinesisExampleWithPreferredLocation {
  def getCoreNodes(): Iterator[String] = {

    val conf = new YarnConfiguration();
    val yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    val corenodes = Iterator
      .continually(
        yarnClient
          .getNodeReports(NodeState.RUNNING)
          .asScala
          .filter(x => x.getNodeLabels().contains("CORE"))
          .map(x => x.getNodeId.getHost.toString)
          .toSet
      )
      .flatten
    corenodes
  }

  def main(args: Array[String]) {

    val appName = args(0)
    var streamName = args(1)
    var regionName = args(2)

    val conf = new SparkConf().setAppName(appName)
    val batchInterval = Milliseconds(1000)
    val ssc = new StreamingContext(conf, batchInterval)
    val cwListener = new CloudWatchSparkListener(appName)
    val endpointUrl = "kinesis." + regionName + ".amazonaws.com"

    val kinesisClient = new AmazonKinesisClient()
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient
      .describeStream(streamName)
      .getStreamDescription()
      .getShards()
      .size
    val numReceivers = numShards

    val kinesisCheckpointInterval = batchInterval * 10

    ssc.addStreamingListener(cwListener)

    // Kinesis DStreams
    val corenodes = getCoreNodes()
    val kinesisStreams = (0 until numReceivers).map { i =>
      CustomKinesisInputDStream.builder
        .withPreferredLocation(corenodes.next)
        .streamingContext(ssc)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .streamName(streamName)
        .initialPositionInStream(InitialPositionInStream.LATEST)
        .checkpointAppName(appName)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }
    // Union all the streams (in case numStreams > 1)
    val unionStreams = ssc.union(kinesisStreams)

    val words =
      unionStreams.flatMap(byteArray => new String(byteArray).split(" "))

    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
