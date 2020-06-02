// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.awslabs.utils;

import org.apache.log4j.Logger;

import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

import scala.collection.mutable.Map
import java.util.ArrayList


class CloudWatchMetricsUtil( var appName: String ) {
	val cw = AmazonCloudWatchClientBuilder.defaultClient();
	val log = Logger.getLogger(getClass.getName)

			val jobFlowInfoFile = "/mnt/var/lib/info/job-flow.json"
			val jobFlowId = getJobFlowId()
			def parseJsonWithJackson(json: BufferedSource): Map[String, Object] = {
					val attrMapper = new ObjectMapper() with ScalaObjectMapper
							attrMapper.registerModule(DefaultScalaModule)
							attrMapper.readValue[Map[String, Object]](json.reader())
	}

	def getJobFlowId(): String = {
			parseJsonWithJackson(Source.fromFile(jobFlowInfoFile)).get("jobFlowId").mkString 
	}

	def sendHeartBeat(dimentionItems: Map[String,String]) : Unit = {
			sendHeartBeat(dimentionItems, "heartBeat")
	}

	def sendHeartBeat(dimentionItems : Map[String,String] , metricName: String) : Unit = {
			pushMetric(dimentionItems,"heartBeat",1.0, StandardUnit.Count);
	}

	def pushMetric(dimentionItems : Map[String,String] , metricName: String, value : Double, unit: StandardUnit ) {
		val dimentions = new ArrayList[Dimension]();

		for ((k,v) <- dimentionItems) {
			var dimension = new Dimension().withName(k).withValue(v);
			dimentions.add(dimension);
		}

		var dimensionAppName = new Dimension()
				.withName("ApplicationName")
				.withValue(appName);

		dimentions.add(dimensionAppName);

		var dimentionsJobFlowId = new Dimension()
				.withName("JobFlowId")
				.withValue(jobFlowId);

		dimentions.add(dimentionsJobFlowId);

		var datum = new MetricDatum()
				.withMetricName(metricName)
				.withUnit(unit)
				.withValue(value)
				.withDimensions(dimentions);

		var request = new PutMetricDataRequest()
				.withNamespace("AWS/ElasticMapReduce")
				.withMetricData(datum);

		val response = cw.putMetricData(request);
		if (response.getSdkHttpMetadata.getHttpStatusCode != 200) {
			log.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata.getRequestId());
			log.debug("Response Status code: " + response.getSdkHttpMetadata.getHttpStatusCode());
		}
	}

	def pushCountMetric(dimentionItems:  Map[String,String] , metricName: String, value: Double) {
		pushMetric(dimentionItems,metricName, value.doubleValue(),StandardUnit.Count);
	}

	def pushMillisecondsMetric(dimentionItems: Map[String, String], metricName: String, value: Long) {
		pushMetric(dimentionItems,metricName, value.longValue(),StandardUnit.Milliseconds);
	}

	def pushSecondsMetric(dimentionItems:  Map[String,String], metricName: String, value: Long) {
		pushMetric(dimentionItems,metricName, value.longValue(),StandardUnit.Seconds);
	}
	
	def pushCountSecondMetric(dimentionItems:  Map[String,String], metricName: String, value: Double ) {
	  pushMetric(dimentionItems,metricName, value.doubleValue(),StandardUnit.CountSecond);
	}
}
