# sender-awssdk-sqs

This component leverages [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java)
interfaces to send spans to Amazon SQS using the [V2 AWS SDK](https://github.com/aws/aws-sdk-java-v2) 
for collection and processing. SQS is an alternative to kafka that is fully managed in the AWS cloud.

## Configuration

A minimal configuration of this sender would be:

```java
sender = SQSSender.create("my-queue");
```

Additionally, [`SqsClient`](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sqs/SqsClient.html) can be customized as needed.

```java
sqsClient = SqsClient.builder()
      .httpClient(UrlConnectionHttpClient.create())
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("my-queue"))
      .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .build();

sender = SQSSender.builder()
    .queueUrl("my-queue")
    .sqsClient(sqsClient)
    .build();
```

There is also an asynchronous variant that uses the `SqsAsyncClient`

```java
sender = SQSAsyncSender.create("my-queue")
```

Additionally, [`SqsAsyncClient`](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sqs/SqsAsyncClient.html) can be customized as needed.

```java
sqsClient = SqsAsyncClient.builder()
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("my-queue"))
      .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .build();

sender = SQSAsyncSender.builder()
    .queueUrl("my-queue")
    .sqsClient(sqsClient)
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