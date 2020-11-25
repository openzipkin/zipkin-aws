# collector-kinesis

## KinesisCollector

The Zipkin Amazon Kinesis Collector is an alternative to the Zipkin Kafka Collector.
The [Amazon Kinesis](https://aws.amazon.com/kinesis/) service is a managed message streaming
service that is effective at decoupling components of cloud applications.  Using it
in place of Kafka removes the need to stand up and manage a distributed Kafka 
deployment.

The Kinesis collector is based on the Java Kinesis Client Library ([KCL](https://github.com/awslabs/amazon-kinesis-client)). This library works much the
same way that Kafka does when there are multiple consumers of the same stream. In the place of
zookeeper for kafka, the KCL uses DynamoDB to do its coordination.

To fully take advantage of the KinesisCollector users will send Thrift encoded Zipkin Spans
directly to a Kinesis stream from each service. 

## Usage

While the KinesisCollector can be used directly through the provided builder interface,
most users will likely find more value in the Spring Boot autoconfiguraton module. 
Additional information for using the module can be found 
[here](https://github.com/openzipkin/zipkin-aws/tree/master/module/collector-kinesis).

## Permissions

The permissions required for the KCL are covered in the AWS documentation [here](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-iam.html).

## Message encoding
The message body is a list of spans. Supported encodings are the same as
the [POST /spans](http://zipkin.io/zipkin-api/#/paths/%252Fspans) body: thrift or json.
