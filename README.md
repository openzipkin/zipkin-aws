# zipkin-aws
Shared libraries that provide Zipkin integration with AWS SQS and SNS. Requires JRE 6 or later.

# Usage
These components provide Zipkin Senders and Collectors which build off interfaces provided by
the [zipkin-reporters-java](https://github.com/openzipkin/zipkin-reporter-java) and
[zipkin](https://github.com/openzipkin/zipkin) projects.

## Senders

### AwsBufferedSqsSender

Sends Spans to SQS using the AwsBufferedAsyncClient. This client maintains a queue and thread for
buffering API requests to the SQS API. By default the queue will send buffered spans every 200ms or
when the AWS batch request limit is reached.

Credentials are provided using the DefaultAwsCredentialsProviderChain by default but can be changed
 through the `credentialsProvider` property which accepts an AwsCredentialsProvider.

```java
reporter = AsyncReporter.builder(
  AwsBufferedSqsSender.create("http://sqs.us-east-1.amazonaws.com/123456789012/queue2")).build();
```

#### Properties

`queueUrl` | SQS queue URL to send spans.
`credentialsProvider` | AwsCredentialsProvider to use for SQS API calls.
Defaults to DefaultAwsCredentialsProviderChain
`encoding` | Span encoding to use, either JSON or Thrift, defaults to Thrift

### AwsSnsSender
TODO

## Collectors

### AwsSqsCollector
TODO

