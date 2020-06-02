package org.apache.spark.streaming.kinesis


import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.internal.Logging
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.AtTimestamp
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.Utils

private[kinesis] class CustomKinesisReceiver[T](
    val sName: String,
    eUrl: String,
    rName: String,
    iPosition: KinesisInitialPosition,
    cAppName: String,
    cInterval: Duration,
    sLevel: StorageLevel,
    mHandler: Record => T,
    kCreds: SparkAWSCredentials,
    dDBCreds: Option[SparkAWSCredentials],
    cWatchCreds: Option[SparkAWSCredentials],
    mLevel: MetricsLevel,
    mEnabledDimensions: Set[String],
    pLocation: Option[String])
  extends KinesisReceiver[T](sName,eUrl,rName,iPosition,cAppName,cInterval,sLevel,mHandler,kCreds,dDBCreds,cWatchCreds) {

  override def preferredLocation: Option[String] = Some(pLocation.getOrElse("NotMatchingName") )
}

