[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://circleci.com/gh/openzipkin/zipkin-aws.svg?style=svg)](https://circleci.com/gh/openzipkin/zipkin-aws)
[![Download](https://api.bintray.com/packages/openzipkin/maven/zipkin-aws/images/download.svg)](https://bintray.com/openzipkin/maven/zipkin-aws/_latestVersion)

# zipkin-aws
Shared libraries that provide Zipkin integration with AWS SQS and SNS. Requires JRE 6 or later.

# Usage
These components provide Zipkin Senders and Collectors which build off interfaces provided by
the [zipkin-reporters-java](https://github.com/openzipkin/zipkin-reporter-java) and
[zipkin](https://github.com/openzipkin/zipkin) projects.

## Senders

### SQSSender

Sends Spans to SQS using the AwsBufferedAsyncClient. This client maintains a queue and thread for
buffering API requests to the SQS API. By default the queue will send buffered spans every 200ms or
when the AWS batch request limit is reached.

Credentials are provided using the DefaultAwsCredentialsProviderChain by default but can be changed
 through the `credentialsProvider` property which accepts an AwsCredentialsProvider.

```java
reporter = AsyncReporter.builder(
  SQSSender.create("https://sqs.us-east-1.amazonaws.com/123456789012/queue")).build();
```

#### Properties

`queueUrl`            | SQS queue URL to send spans.
`credentialsProvider` | AwsCredentialsProvider to use for SQS API calls. Defaults to
 DefaultAwsCredentialsProviderChain

#### Message Format

This sender only sends Thrift encoded Spans as base64 strings in the SQS message body.

## Collectors

### SQSCollector

Collects Spans from SQS using the AmazonSQSAsyncClient. By default this client uses long polling 
and will wait on a response for up to 20 seconds. After messages are received or the wait time is
reached the client will start a new request.  Messages that are accepted by the collector are
deleted from SQS after successfully storing them.

```java
new SQSCollector.Builder()
  .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/queue")
  .metrics(CollectorMetrics.NOOP_METRICS)
  .sampler(CollectorSampler.ALWAY_SAMPLE)
  .storage(new InMemoryStorage())
  .build()
  .start()
```

