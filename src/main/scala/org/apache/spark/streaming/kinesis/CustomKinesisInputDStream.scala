package org.apache.spark.streaming.kinesis

import scala.reflect.ClassTag

import collection.JavaConverters._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo

private[kinesis] class CustomKinesisInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    val sName: String,
    val eUrl: String,
    val rName: String,
    val iPosition: KinesisInitialPosition,
    val cAppName: String,
    val cInterval: Duration,
    val sLevel: StorageLevel,
    val mHandler: Record => T,
    val kCreds: SparkAWSCredentials,
    val dDBCreds: Option[SparkAWSCredentials],
    val cWatchCreds: Option[SparkAWSCredentials],
    val mLevel: MetricsLevel,
    val mEnabledDimensions: Set[String],
    val preferredLocation : Option[String]
  ) extends KinesisInputDStream[T](_ssc,sName,eUrl,rName,iPosition,cAppName,cInterval,sLevel,mHandler,kCreds,dDBCreds,cWatchCreds) {
  
  override def getReceiver(): Receiver[T] = {
    new CustomKinesisReceiver(sName, eUrl, rName, iPosition,
      cAppName, cInterval, sLevel, mHandler,
      kCreds, dDBCreds, cWatchCreds,
      mLevel, mEnabledDimensions,preferredLocation)
  }

}

object CustomKinesisInputDStream {
    class Builder extends KinesisInputDStream.Builder {
        private var preferredLocation : Option[String] = None 

        private var streamingContext: Option[StreamingContext] = None
        private var streamName: Option[String] = None
        private var checkpointAppName: Option[String] = None
    
        // Params with defaults
        private var endpointUrl: Option[String] = None
        private var regionName: Option[String] = None
        private var initialPosition: Option[KinesisInitialPosition] = None
        private var checkpointInterval: Option[Duration] = None
        private var storageLevel: Option[StorageLevel] = None
        private var kinesisCredsProvider: Option[SparkAWSCredentials] = None
        private var dynamoDBCredsProvider: Option[SparkAWSCredentials] = None
        private var cloudWatchCredsProvider: Option[SparkAWSCredentials] = None
        private var metricsLevel: Option[MetricsLevel] = None
        private var metricsEnabledDimensions: Option[Set[String]] = None

        def withPreferredLocation(hostname : String ) : Builder = {
          this.preferredLocation = Option(hostname)
          this
        }  

    override def streamingContext(ssc: StreamingContext): Builder = {
    		streamingContext = Option(ssc)
    				this
    }

    override def streamingContext(jssc: JavaStreamingContext): Builder = {
    		streamingContext = Option(jssc.ssc)
    				this
    }

    override def streamName(streamName: String): Builder = {
    		this.streamName = Option(streamName)
    				this
    }

    override def checkpointAppName(appName: String): Builder = {
    		checkpointAppName = Option(appName)
    				this
    }

    override def endpointUrl(url: String): Builder = {
    		endpointUrl = Option(url)
    				this
    }

    override def regionName(regionName: String): Builder = {
    		this.regionName = Option(regionName)
    				this
    }

    override def initialPosition(initialPosition: KinesisInitialPosition): Builder = {
    		this.initialPosition = Option(initialPosition)
    				this
    }

    override def checkpointInterval(interval: Duration): Builder = {
    		checkpointInterval = Option(interval)
    				this
    }

    override def storageLevel(storageLevel: StorageLevel): Builder = {
    		this.storageLevel = Option(storageLevel)
    				this
    }

    override def kinesisCredentials(credentials: SparkAWSCredentials): Builder = {
    		kinesisCredsProvider = Option(credentials)
    				this
    }

    override def dynamoDBCredentials(credentials: SparkAWSCredentials): Builder = {
    		dynamoDBCredsProvider = Option(credentials)
    				this
    }

    override def cloudWatchCredentials(credentials: SparkAWSCredentials): Builder = {
    		cloudWatchCredsProvider = Option(credentials)
    				this
    }

    def metricsLevel(metricsLevel: MetricsLevel): Builder = {
    		this.metricsLevel = Option(metricsLevel)
    				this
    }

    def metricsEnabledDimensions(metricsEnabledDimensions: Set[String]): Builder = {
    		this.metricsEnabledDimensions = Option(metricsEnabledDimensions)
    				this
    } 
        protected def getRequiredParam[T](param: Option[T], paramName: String): T = param.getOrElse {
            throw new IllegalArgumentException(s"No value provided for required parameter $paramName")
        }

        def customBuildWithMessageHandler[T: ClassTag]( handler: Record => T): CustomKinesisInputDStream[T] = {
          val ssc = getRequiredParam(streamingContext, "streamingContext")
          new CustomKinesisInputDStream(
          ssc,
          getRequiredParam(streamName, "streamName"),
          endpointUrl.getOrElse(DEFAULT_KINESIS_ENDPOINT_URL),
          regionName.getOrElse(DEFAULT_KINESIS_REGION_NAME),
          initialPosition.getOrElse(DEFAULT_INITIAL_POSITION),
          getRequiredParam(checkpointAppName, "checkpointAppName"),
          checkpointInterval.getOrElse(ssc.graph.batchDuration),
          storageLevel.getOrElse(DEFAULT_STORAGE_LEVEL),
          ssc.sc.clean(handler),
          kinesisCredsProvider.getOrElse(DefaultCredentials),
          dynamoDBCredsProvider,
          cloudWatchCredsProvider,
          metricsLevel.getOrElse(DEFAULT_METRICS_LEVEL),
          metricsEnabledDimensions.getOrElse(DEFAULT_METRICS_ENABLED_DIMENSIONS),
          preferredLocation)
        }
        override def build(): CustomKinesisInputDStream[Array[Byte]] = customBuildWithMessageHandler(defaultMessageHandler)
    }
    val builder = new CustomKinesisInputDStream.Builder;

    private[kinesis] def defaultMessageHandler(record: Record): Array[Byte] = {
      if (record == null) return null
      val byteBuffer = record.getData()
      val byteArray = new Array[Byte](byteBuffer.remaining())
      byteBuffer.get(byteArray)
      byteArray
    }
    protected[kinesis] val DEFAULT_KINESIS_ENDPOINT_URL: String =
    "https://kinesis.us-east-1.amazonaws.com"
    protected[kinesis] val DEFAULT_KINESIS_REGION_NAME: String = "us-east-1"
    protected[kinesis] val DEFAULT_INITIAL_POSITION: KinesisInitialPosition = new Latest()
    protected[kinesis] val DEFAULT_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK_2
    protected[kinesis] val DEFAULT_METRICS_LEVEL: MetricsLevel =
      KinesisClientLibConfiguration.DEFAULT_METRICS_LEVEL
    protected[kinesis] val DEFAULT_METRICS_ENABLED_DIMENSIONS: Set[String] =
      KinesisClientLibConfiguration.DEFAULT_METRICS_ENABLED_DIMENSIONS.asScala.toSet
}
