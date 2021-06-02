This project is a Custom Spark Listener to push spark streaming metrics to AWS CloudWatch. 

To run with our custom Spark listener with the sample Spark Kinesis streaming application, we should have the listener in the classpath. In this case, our Custom SparkListener is a part of our project Jar file.

To run the Kinesis streaming example:

spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.1 --class com.amazonaws.awslabs.sparkstreaming.SparkKinesisExample $CP/aws-cloudwatch-metrics-custom-spark-listener/target/scala-2.12/sparkkinesisexample_2.12-0.2.jar $APPNAME $STREAMNAME $REGION

To run the WordCount Kafka streaming example:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --class com.amazonaws.awslabs.sparkstreaming.SparkKafkaExample $CP/aws-cloudwatch-metrics-custom-spark-listener/target/scala-2.12/sparkkinesisexample_2.12-0.2.jar $APPNAME $BOOSTRAPSERVERS $TOPIC

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
