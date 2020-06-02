// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.awslabs.sparkstreaming;

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.amazonaws.awslabs.sparkstreaming.listener._

import com.amazonaws.awslabs.sparkstreaming.listener._
import com.amazonaws.awslabs.utils._
object SparkKafkaExample {

  def main(args: Array[String]) {

    val appName = args(0)
    var bootstrapservers = args(1)
    var topic = args(2)
    val spark = SparkSession.builder.appName(appName).getOrCreate()

    import spark.implicits._
    val cwListener = new CloudWatchQueryListener(appName)
    spark.streams.addListener(cwListener)

    val lines = spark.readStream.format("kafka")
                     .option("kafka.bootstrap.servers",bootstrapservers)
                     .option("subscribe", topic)
                     .load()
                     .selectExpr("CAST(value AS STRING)")
                     .as[String]

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }
}
