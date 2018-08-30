# sender-sqs

This component leverages [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java)
interfaces to send spans to Amazon SQS for collection and processing.
SQS is an alternative to kafka that is fully managed in the AWS cloud.

## Configuration

A minimal configuration of this sender would be:

```java
sender = SQSSender.create("my-queue");
```

Additionally, `CredentialsProvider`, `EndpointConfiguration` and `region`
can all be customized as needed.

```java
sender = SQSSender.builder()
    .queueName("my-queue")
    .region("us-west-1")
    .credentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
    .endpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8080/", "us-east-1"))
    .build();
```

## Requirements

The credentials that your service has requires the following permissions in order to function:

`sqs:DescribeStream` for health checking

`sqs:PutRecord` for placing spans on the queue

## Message encoding
The message's binary data includes a list of spans. Supported encodings
are the same as the http [POST /spans](http://zipkin.io/zipkin-api/#/paths/%252Fspans) body.

Encoding defaults to json, but can be overridden to use PROTO3 instead.

Unless the message is ascii, it is Base64 encoded before being sent. For
example, plain json is sent as is. Thrift or json messages that include
unicode characters are encoded using Base64.

# Related work

[collector-sqs](https://github.com/openzipkin/zipkin-aws/tree/master/collector-sqs)
integrates with zipkin-server to pull spans off of an SQS queue instead
of http or kafka.