# AWS SQS V2 Messaging Instrumentation

This module contains instrumentation for AWS SDK V2 `SqsClient` `SendMessage*` request types.

The `SqsMessageTracing` type provides an `ExecutionInterceptor` instance that can be added to your
clients at build time. It adds tracing headers to both `SendMessageRequest` and
`SendMessageBatchRequest` types. In the case of `SendMessageBatchRequest` each message in the batch
gets its own `Span` and headers added.

## Usage

You will want to create your SQS client and add the execution interceptor:

```java
Tracing tracing = ...;
SqsMessageTracing sqsMessageTracing = SqsMessageTracing.create(tracing);

ClientOverrideConfiguration configuration = ClientOverrideConfiguration.builder()
    .addExecutionInterceptor(sqsMessageTracing.executionInterceptor())
    .build();

SqsClient client = SqsClient.builder()
    .overrideConfiguration(configuration)
    .build();
```

Now use your SQS client as you normally would and spans will be attached to outgoing messages.

## Notes

* This is the V2 SDK equivalent of [instrumentation-aws-java-sdk-sqs](../instrumentation-aws-java-sdk-sqs).
* This can be combined with [instrumentation-aws-java-sdk-v2-core](../instrumentation-aws-java-sdk-v2-core) on the same client to get both HTTP-level CLIENT spans and message-level PRODUCER spans.
