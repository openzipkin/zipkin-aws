# collector-kinesis

## KinesisCollector

The Zipkin Amazon Kinesis Collector is an alternative to the Zipkin Kafka Collector.
The [Amazon Kinesis](https://aws.amazon.com/kinesis/) service is a managed message streaming
service that is effective at decoupling components of cloud applications.  Using it
in place of Kafka removes the need to stand up and manage a distributed Kafka 
deployment.

To fully take advantage of the KinesisCollector users will send Thrift encoded Zipkin Spans
directly to a Kinesis stream from each service. 

## Usage

While the KinesisCollector can be used directly through the provided builder interface,
most users will likely find more value in the Spring Boot autoconfiguraton module. 
Additional information for using the module can be found 
[here](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/collector-kinesis).
