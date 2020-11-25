# AWS SDK Instrumentation

This module contains instrumentation for [AWS](https://github.com/aws/aws-sdk-java) clients that
extend `AmazonWebServiceClient`.

The `AwsClientTracing` class finalizes your `AwsClientBuilder` instances by adding a
`RequestHandler2` instance to your client and wrapping the `ExecutorService` in the case of an async
client.

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
AmazonDynamoDBAsyncClientBuilder builder = AmazonDynamoDBAsyncClientBuilder.standard();
AmazonDynamoDB client = AwsClientTracing.create(httpTracing).build(clientBuilder);

// Now use you client like usual
```
