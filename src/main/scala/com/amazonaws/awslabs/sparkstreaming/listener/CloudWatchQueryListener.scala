// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.awslabs.sparkstreaming.listener;

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.log4j.Logger
import org.joda.time.DateTime
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import collection.mutable.{ Map , HashMap }
import scala.collection.JavaConverters._


import com.amazonaws.awslabs.utils._

class CloudWatchQueryListener(appName: String = "ApplicationName") extends StreamingQueryListener {
  
  val log = Logger.getLogger(getClass.getName)
  val metricUtil = new CloudWatchMetricsUtil(appName)
  val dimentionsMap = new HashMap[String,String]()
 
  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      println("Query started: " + queryStarted.id)
      metricUtil.sendHeartBeat(dimentionsMap,"queryStarted")
  }
  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + queryTerminated.id)
  }
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      println("Query made progress: " + queryProgress.progress)
      writeQueryStatsToCloudWatch(queryProgress)
  }
 
  def writeQueryStatsToCloudWatch(queryProgress: QueryProgressEvent): Unit = {
    val progress = queryProgress.progress
    if ( progress.inputRowsPerSecond.isNaN == false ) {
      metricUtil.pushCountSecondMetric(dimentionsMap, "inputRowsPerSecond", progress.inputRowsPerSecond)
    }
    if ( progress.processedRowsPerSecond.isNaN == false ) {
      metricUtil.pushCountSecondMetric(dimentionsMap, "processedRowsPerSecond", progress.processedRowsPerSecond)
    }

    metricUtil.pushCountMetric(dimentionsMap, "numInputRows",progress.numInputRows)

    for ((k ,v ) <- progress.durationMs.asScala) {
      metricUtil.pushMillisecondsMetric(dimentionsMap, k, v)
    }
  }
}
