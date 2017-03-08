# sparkstreaming-stream-kinesis

## KinesisStreamFactory

The Zipkin Amazon Kinesis Stream provider for the [zipkin-sparkstreaming](https://github.com/openzipkin/zipkin-sparkstreaming)
project is an alternative to the Kafka Stream provider.
The [Amazon Kinesis](https://aws.amazon.com/kinesis/) service is a managed streaming data
service that is effective at enabling analysis of large amounts of data.  Using it
in place of Kafka removes the need to stand up and manage a distributed Kafka 
deployment.

## Usage

While the KinesisStreamFactory can be used directly through the provided builder interface,
most users will likely find more value in the Spring Boot autoconfiguraton module. 
Additional information for using the module can be found in that modules
[README](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/sparkstreaming-stream-kinesis).

