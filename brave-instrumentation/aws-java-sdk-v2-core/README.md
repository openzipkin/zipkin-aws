# AWS SDK V2 Instrumentation

This module contains instrumentation for [AWS](https://github.com/aws/aws-sdk-java-v2) clients that
extend `SdkClient`.

The `TracingClientOverrideConfiguration` class finalizes your `ClientOverrideConfiguration`
instances by adding a `ExecutionInterceptor` instance to your client configuration.

## Span Model

Traces AWS Java SDK calls. Adds on the standard zipkin/brave http tags, as well as tags that align
with the XRay data model.

This implementation creates 2 types of spans to allow for better error visibility.

The outer span, "Application Span", wraps the whole SDK operation. This span uses `aws-sdk` as it's
name and will NOT have a remoteService configuration, making it a local span. If the entire
operation results in an error then this span will have an error tag with the cause.

The inner span, "Client Span", is created for each outgoing HTTP request. This span will be of type
CLIENT. The remoteService will be the name of the AWS service, and the span name will be the name of
the operation being done. If the request results in an error then the span will be tagged with the
error. The AWS request ID is added when available.

## Wiring it up

```java
// Set up brave
Tracing tracing = Tracing.currentTracer();
HttpTracing httpTracing = HttpTracing.create(tracing);

// Create your client
ClientOverrideConfiguration configuration =
    TracingClientOverrideConfiguration.create(httpTracing).build(ClientOverrideConfiguration.builder());
DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().overrideConfiguration(configuration).build();

// Now use you client like usual
```
