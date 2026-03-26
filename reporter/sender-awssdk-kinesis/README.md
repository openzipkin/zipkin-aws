# sender-awssdk-kinesis

This component leverages [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java)
interfaces to send spans to Amazon Kinesis using the [V2 AWS SDK](https://github.com/aws/aws-sdk-java-v2)
for collection and processing. Kinesis is an alternative to kafka that is fully managed in the AWS
cloud.

## Configuration

A minimal configuration of this sender would be:

```java
sender = KinesisSender.create("my-stream");
```

Additionally, [`KinesisClient`](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/kinesis/KinesisClient.html) can be customized as needed.

```java
kinesisClient = KinesisClient.builder()
      .httpClient(UrlConnectionHttpClient.create())
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("http://localhost:4566"))
      .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .build();

sender = KinesisSender.newBuilder()
    .streamName("my-stream")
    .kinesisClient(kinesisClient)
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

# Related work

[collector-kinesis](https://github.com/openzipkin/zipkin-aws/tree/master/collector/kinesis)
integrates with zipkin-server to pull spans off of a Kinesis stream
instead of http or kafka.
