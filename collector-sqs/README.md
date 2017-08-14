# collector-sqs

## SQSCollector

The Zipkin Amazon SQS Collector is an alternative to the Zipkin Kafka Collector.
The [Amazon SQS](https://aws.amazon.com/sqs/) service is a managed message queuing
service that is effective at decoupling components of cloud applications.  Using it
in place of Kafka removes the need to stand up and manage a distributed Kafka 
deployment.

To fully take advantage of the SQSCollector users will typically send Zipkin Spans
directly to a regional SQS queue from each cloud service.  For Java applications we 
provide the [SQS Zipkin Reporter](https://github.com/openzipkin/zipkin-aws/tree/master/sender-sqs) 
that can be used with any tracing backend that accepts a [Zipkin Reporter](https://github.com/openzipkin/zipkin-reporter-java) 

## Usage

While the SQSCollector can be used directly through the provided builder interface,
most users will likely find more value in the Spring Boot autoconfiguraton module. 
Additional information for using the module can be found 
[here](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/collector-sqs).

## Message encoding
The message body is a list of spans. Supported encodings are the same as
the [POST /spans](http://zipkin.io/zipkin-api/#/paths/%252Fspans) body: thrift or json.

Unless the message is ascii, it must be Base64 encoded before being
sent. For example, plain json can be sent as is. Thrift or json messages
that include unicode characters must be encoded using Base64.
