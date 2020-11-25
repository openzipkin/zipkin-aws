# sender-kinesis

This component leverages [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java)
interfaces to send spans to Amazon Kinesis for collection and processing.
Kinesis is an alternative to kafka that is fully managed in the AWS cloud.

## Configuration

A minimal configuration of this sender would be:

```java
sender = KinesisSender.create("my-stream");
```

Additionally, `CredentialsProvider`, `EndpointConfiguration` and `region`
can all be customized as needed.

```java
sender = KinesisSender.builder()
    .streamName("my-stream")
    .region("us-west-1")
    .credentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
    .endpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8080/", "us-east-1"))
    .build();
```

## Requirements

The credentials that your service has requires the following permissions in order to function:

`kinesis:DescribeStream` for health checking

`kinesis:PutRecord` for placing spans on the stream

## Message encoding
The message's binary data includes a list of spans. Supported encodings
are the same as the http [POST /spans](http://zipkin.io/zipkin-api/#/paths/%252Fspans) body.

Encoding defaults to json, but can be overridden to PROTO3 if required.

Note: Span encoding happens before Kinesis Base64 data encoding.
For example, if you look at a raw `PutRecord` request, the `Data` field
will always be [Base64 encoded](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#API_PutRecord_RequestSyntax).

# Related work

[collector-kinesis](https://github.com/openzipkin/zipkin-aws/tree/master/collector-kinesis)
integrates with zipkin-server to pull spans off of a Kinesis stream
instead of http or kafka.

[sparkstreaming-stream-kinesis](https://github.com/openzipkin/zipkin-aws/tree/master/sparkstreaming-stream-kinesis)
is a module for the [zipkin-sparkstreaming](https://github.com/openzipkin/zipkin-sparkstreaming)
project that allows to reading the spans and running real-time spark processing on them.