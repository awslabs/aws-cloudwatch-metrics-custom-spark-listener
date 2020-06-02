// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.awslabs.sparkstreaming.listener;

import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.log4j.Logger
import org.joda.time.DateTime
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import collection.mutable.{ Map , HashMap }

import com.amazonaws.awslabs.utils._

class CloudWatchSparkListener(appName: String = "ApplicationName") extends StreamingListener {
  
  val log = Logger.getLogger(getClass.getName)
  val metricUtil = new CloudWatchMetricsUtil(appName)
  val dimentionsMap = new HashMap[String,String]()
 
  /**
    * This method executes when a Spark Streaming batch completes.
    *
    * @param batchCompleted Class having information on the completed batch
    */

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    log.info("CloudWatch Streaming Listener, onBatchCompleted:" + appName)

    // write performance metrics to CloutWatch Metrics
    writeBatchStatsToCloudWatch(batchCompleted)

  }
  /**
  * This method executes when a Spark Streaming batch completes.
  *
  * @param receiverError Class having information on the reciever Errors
  */

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = { 
    log.warn("CloudWatch Streaming Listener, onReceiverError:" + appName)

    writeRecieverStatsToCloudWatch(receiverError)
  }

    /**
    * This method will just send one, whenever there is a recieverError
    * @param receiverError Class having information on the completed batch
    */
  def writeRecieverStatsToCloudWatch(receiverError: StreamingListenerReceiverError): Unit = {

    metricUtil.sendHeartBeat(dimentionsMap,"receiverError")

  }

  /**
    * Processing Time: How many seconds it took to complete this batch (i.e. duration).
    * Scheduling Delay: How many seconds the start time of this bach was delayed from when the Driver scheduled it.
    * Num Records: The total number of input records in this batch.
    *
    * @param batch Class having information on the completed batch
    */
  def writeBatchStatsToCloudWatch(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    val processingTime = if (batchCompleted.batchInfo.processingDelay.isDefined) {
      batchCompleted.batchInfo.processingDelay.get 
    }

    val schedulingDelay = if (batchCompleted.batchInfo.schedulingDelay.isDefined && batchCompleted.batchInfo.schedulingDelay.get > 0 ) {
       batchCompleted.batchInfo.schedulingDelay.get 
    } else { 0 }

    val numRecords = batchCompleted.batchInfo.numRecords

    metricUtil.sendHeartBeat(dimentionsMap,"batchCompleted")
    metricUtil.pushMillisecondsMetric(dimentionsMap, "schedulingDelay", schedulingDelay )
    metricUtil.pushMillisecondsMetric(dimentionsMap, "processingDelay", batchCompleted.batchInfo.processingDelay.get )
    metricUtil.pushCountMetric(dimentionsMap, "numRecords", numRecords)
    metricUtil.pushMillisecondsMetric(dimentionsMap, "totalDelay", batchCompleted.batchInfo.totalDelay.get);

    log.info("Batch completed at: " + batchCompleted.batchInfo.processingEndTime.get +
             " was started at: " + batchCompleted.batchInfo.processingStartTime.get + 
             " submission time: " + batchCompleted.batchInfo.submissionTime +
             " batch time: " + batchCompleted.batchInfo.batchTime + 
             " batch processing delay: " + batchCompleted.batchInfo.processingDelay.get + 
             " records : " + numRecords +
             " total batch delay:" + batchCompleted.batchInfo.totalDelay.get + 
             " product prefix:" + batchCompleted.batchInfo.productPrefix +
             " schedulingDelay:" + schedulingDelay +
             " processingTime:" + processingTime 
             )
  }
}
