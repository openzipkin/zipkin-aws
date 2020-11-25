# AWS SDK V2 Instrumentation

This module contains instrumentation for [AWS](https://github.com/aws/aws-sdk-java-v2) clients that
extend `SdkClient`.

The `AwsSdkTracing` class produces an `ExecutionInterceptor` that you will add to your
`ClientOverrideConfiguration` instances in order to trace them.

## Span Model

Traces AWS Java SDK calls. Adds on the standard zipkin/brave http tags, as well as tags that align
with the XRay data model.

This implementation creates 2 types of spans to allow for better error visibility.

The outer span, "Application Span", wraps the whole SDK operation. This span uses `aws-sdk` as it's
name and will NOT have a remoteService configuration, making it a local span. If the entire
operation results in an error then this span will have an error tag with the cause.

The inner span, "Client Span", is created for each outgoing HTTP request. This span will be of type
CLIENT. The remoteService will be the name of the AWS service, and the span name will be the name of
the operation being done.

## Wiring it up

```java
// Set up brave
Tracing tracing = Tracing.currentTracer();
HttpTracing httpTracing = HttpTracing.create(tracing);
AwsSdkTracing awsSdkTracing = AwsSdkTracing.create(httpTracing);

// Create your client
ClientOverrideConfiguration configuration = ClientOverrideConfiguration.builder()
        // Any other options you'd like to set
        .addExecutionInterceptor(awsSdkTracing.executionInterceptor())
        .build();
DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
        .overrideConfiguration(configuration)
        .build();

// Now use you client like usual
```
