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

### SNSSender
TODO

## Collectors

### SQSCollector

Collects Spans from SQS using the AwsBufferedAsyncClient. This client maintains a queue and thread
for buffering API requests and pre-fetching messages. By default this client will block a request
for up to 20 seconds while waiting for messages. After messages are received or the wait time is
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

