# sender-kinesis

This component leverages [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java)
interfaces to send spans to Amazon Kinesis for collection and processing. Kinesis is an alternative
to kafka that is fully managed in the AWS cloud.

## Configuration

A minimal configuration of this sender would be:

```java
sender = KinesisSender.builder().streamName("my-stream").build();
```

Additionally the `CredentialsProvider`, `EndpointConfiguration`, and `region` can all be customized as
needed.

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

`kinesis:PutRecord` for placing spans on the queue

# Related work

[collector-kinesis](https://github.com/openzipkin/zipkin-aws/tree/master/collector-kinesis) is available
to allow your zipkin server to pull traces off of a kinesis queue in the place of the built in http,
thrift, and kafka.

[sparkstreaming-stream-kinesis](https://github.com/openzipkin/zipkin-aws/tree/master/sparkstreaming-stream-kinesis)
is a module for the [zipkin-sparkstreaming](https://github.com/openzipkin/zipkin-sparkstreaming)
project that allows to reading the spans and running real-time spark processing on them.