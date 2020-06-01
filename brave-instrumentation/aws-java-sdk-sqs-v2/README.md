# AWS SQS V2 Messaging Instrumentation

This module contains instrumentation for the AWS `SqsClient` client `SendMessage*` request types
to add Brave span information into the message attributes of the SQS message.  This allows
consuming services to continue the trace allowing better visibility on how information is
flowing through your services.

There are two `ExecutionInterceptors` that can be included in the `SqsClient` to provide tracing
information based on your needs:
- [SendMessageTracingExecutionInterceptor](./src/main/java/brave/instrumentation/aws/sqs/SendMessageTracingExecutionInterceptor.java):
which hooks into the `sendMessage` request.
-  [SendMessageBatchTracingExecutionInterceptor](./src/main/java/brave/instrumentation/aws/sqs/SendMessageBatchTracingExecutionInterceptor.java):
which hooks into the `sendMessageBatch` request.

## Usage

You will want to create your SQS client and add the request handler

```java
Tracing tracing = ...;
SqsAsyncClient sqsAsyncClient = SqsAsyncClient.builder()
         // configuration for your client
        .overrideConfiguration(builder -> {
          builder.addExecutionInterceptor(new SendMessageTracingExecutionInterceptor(tracing));
          builder.addExecutionInterceptor(new SendMessageBatchTracingExecutionInterceptor(tracing));
        )
        .build();
```

Now use your SQS client as you normally would and spans will be attached to outgoing messages.

### Customizing the Span
The interceptors have their default logic for applying certain tags to the span but if you wish
to provide your own logic you can provide your own `SpanDecorator` implementation which supplies
callbacks to certain events in the message publishing. See each interceptor for more information
about the events that you can hook into.
