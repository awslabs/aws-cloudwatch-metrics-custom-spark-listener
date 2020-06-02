// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//package com.amazonaws.awslabs.kinesis-producer

import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient} 
import com.amazonaws.services.kinesis.model.{DescribeStreamResult, Shard, PutRecordsRequestEntry, PutRecordsRequest}

import com.amazonaws.services.kinesis.model.AmazonKinesisException

import scala.collection.JavaConverters._
import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object ExampleKinesisProducer {
    val usage = """
      Usage: ExampleKinesisProducer stream-name 
    """
 
    val client = AmazonKinesisClientBuilder.defaultClient()
    val thebook = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("ThePickwickPapers.txt")).mkString.split("(?m)(?=^\\s{2})")
    val random = scala.util.Random

    def main(args: Array[String]) = {
      if (args.length == 0) println(usage)
      val arglist = args.toList
      type OptionMap = Map[Symbol, Any]

      def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
        def isSwitch(s : String) = (s(0) == '-')
        list match {
          case Nil => map
          //case "--max-object-size" :: value :: tail =>
          //                       nextOption(map ++ Map('maxsize -> value.toInt), tail)
          //case "--min-object-size" :: value :: tail =>
          //                     nextOption(map ++ Map('minsize -> value.toInt), tail)
          case string :: opt2 :: tail if isSwitch(opt2) => 
                                 nextOption(map ++ Map('streamname -> string), list.tail)
          case string :: Nil =>  nextOption(map ++ Map('streamname -> string), list.tail)
          case option :: tail => println("Unknown option "+option) 
                                 System.exit(1)
                                 Map()
        }
      }
 
      def getShardCount( streamName : String  ) : Integer = {
          val descResult : DescribeStreamResult = client.describeStream(streamName)
          val shards = descResult.getStreamDescription().getShards().asScala
          shards.size
      }

      def getRandomLines() : String = {
         thebook(random.nextInt(thebook.size)) 
      }

      def makePutRecordRequestEntry() : PutRecordsRequestEntry = {
         val charsetEncoder = java.nio.charset.Charset.forName("UTF-8").newEncoder()
         val record = new PutRecordsRequestEntry()
	       record.setData(charsetEncoder.encode(java.nio.CharBuffer.wrap(getRandomLines())))
         record.setPartitionKey(Math.abs(random.nextInt).toString)
         record
      }

      def makePutRecordsRequest( streamName : String ) : PutRecordsRequest = {
         var recordsSize : Integer = 0
         var records = new ListBuffer[PutRecordsRequestEntry]()
         breakable {
            while ( true ) {
            	val record = makePutRecordRequestEntry() 
            	val sz = record.getData().capacity()
            	if  ( ( sz < 1048576) && ( sz + recordsSize < 1048576 ) && records.size < 500 ) {
                	records += record  
                	recordsSize += sz
            	} else {
              	break
            	}
	    }
         }
         val recordRequest = new PutRecordsRequest() 
         recordRequest.setRecords(records.asJavaCollection)
         recordRequest.setStreamName(streamName)
         recordRequest
      }
            

      val options = nextOption(Map(),arglist)
      println(options)

      val kinesisClient : AmazonKinesis = AmazonKinesisClientBuilder.defaultClient()
      println("ShardCount: " + getShardCount(options(Symbol("streamname")).asInstanceOf[String]))
      while ( true ) {
      	try { 
      		kinesisClient.putRecords(makePutRecordsRequest(options(Symbol("streamname")).asInstanceOf[String]))
      	} catch {
		      case e : AmazonKinesisException => {
			      // Do nothing .. We will keep pushing to maximize the thoughput .. Even ignoring throttles.
            println(e.getErrorMessage())
		      } 
      	  }
      }
    }
}
