# AWS SQS Messaging Instrumentation

This module contains instrumentation for AWS `AmazonSQS` client `SendMessage*` request types.

The `SqsMessageTracing` type provides a `RequestHandler2` instance that can be added to your clients
at build time. It adds tracing headers to both `SendMessageRequest` and `SendMessageBatchRequest`
types. In the case of `SendMessageBatchRequest` each message in the batch gets it's own `Span` and
headers added.

## Usage

You will want to create your SQS client and add the request handler

```java
Tracing tracing = ...;
SqsMessageTracing sqsMessageTracing = SqsMessageTracing.create(tracing);

AmazonSQSAsync client = AmazonSQSAsyncClientBuilder.standard()
    .withRequestHandlers(sqsMessageTracing.requestHandler())
    .build();
```

Now use your SQS client as you normally would and spans will be attached to outgoing messages.

**Note:** This does not support `AmazonSQSBufferedAsyncClient` because that class does not respect `RequestHandler2` due to it's implementation buffering individual `sendMessage()` calls and batching them as a single `sendMessageBatch()` call.
