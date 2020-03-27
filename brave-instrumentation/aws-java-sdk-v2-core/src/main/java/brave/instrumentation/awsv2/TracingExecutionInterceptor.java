/*
 * Copyright 2016-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.instrumentation.awsv2;

import brave.Span;
import brave.Tracer;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.instrumentation.awsv2.AwsSdkTracing.HttpClientRequest;
import brave.instrumentation.awsv2.AwsSdkTracing.HttpClientResponse;
import brave.propagation.TraceContext;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Traces AWS Java SDK V2 calls. Adds on the standard zipkin/brave http tags, as well as tags that
 * align with the XRay data model.
 *
 * This implementation creates 2 types of spans to allow for better error visibility.
 *
 * The outer span, "Application Span", wraps the whole SDK operation. This span uses the AWS service
 * as it's name and will NOT have a remoteService configuration, making it a local span. If the
 * entire operation results in an error then this span will have an error tag with the cause.
 *
 * The inner span, "Client Span", is created for each outgoing HTTP request. This span will be of
 * type CLIENT. The remoteService will be the name of the AWS service, and the span name will be the
 * name of the operation being done. If the request results in an error then the span will be tagged
 * with the error. The AWS request ID is added when available.
 */
final class TracingExecutionInterceptor implements ExecutionInterceptor {
  static final ExecutionAttribute<TraceContext> DEFERRED_ROOT_CONTEXT =
      new ExecutionAttribute<>("DEFERRED_ROOT_CONTEXT");
  static final ExecutionAttribute<Span> APPLICATION_SPAN =
      new ExecutionAttribute<>("APPLICATION_SPAN");

  final Tracer tracer;
  final HttpTracing httpTracing;
  final HttpClientHandler<brave.http.HttpClientRequest, brave.http.HttpClientResponse> handler;

  TracingExecutionInterceptor(HttpTracing httpTracing) {
    this.httpTracing = httpTracing;
    this.tracer = httpTracing.tracing().tracer();
    this.handler = HttpClientHandler.create(httpTracing);
  }

  /**
   * Before the SDK request leaves the calling thread
   */
  @Override public void beforeExecution(
      Context.BeforeExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span maybeDeferredRootSpan = tracer.nextSpan();
    if (maybeDeferredRootSpan.context().parentIdAsLong() == 0) {
      // Deferred to sampling when we have http context
      // We will build a new span with this context later when we can make the sampling decision
      executionAttributes.putAttribute(DEFERRED_ROOT_CONTEXT, maybeDeferredRootSpan.context());
    } else {
      executionAttributes.putAttribute(APPLICATION_SPAN, maybeDeferredRootSpan.start());
    }
  }

  /**
   * Before an individual http request happens, may be run multiple times if retries occur
   */
  @Override public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context,
      ExecutionAttributes executionAttributes
  ) {
    TraceContext maybeDeferredRootSpan = executionAttributes.getAttribute(DEFERRED_ROOT_CONTEXT);
    Span span;
    HttpClientRequest request = new HttpClientRequest(context.httpRequest());
    if (maybeDeferredRootSpan != null) {
      Boolean sampled = httpTracing.clientRequestSampler().trySample(request);
      if (sampled == null) {
        sampled = httpTracing.tracing().sampler().isSampled(maybeDeferredRootSpan.traceId());
      }
      span = tracer.toSpan(maybeDeferredRootSpan.toBuilder().sampled(sampled).build());
      executionAttributes.putAttribute(APPLICATION_SPAN, span.start());
    } else {
      span = executionAttributes.getAttribute(APPLICATION_SPAN);
    }

    String serviceName = executionAttributes.getAttribute(SdkExecutionAttribute.SERVICE_NAME);
    String operation = getAwsOperationNameFromRequestClass(context.request());
    span.name("aws-sdk")
        .tag("aws.service_name", serviceName)
        .tag("aws.operation", operation);

    handler.handleSend(request, span);
    span.name(operation).remoteServiceName(serviceName);

    return request.build();
  }

  @Override public void beforeTransmission(Context.BeforeTransmission context,
      ExecutionAttributes executionAttributes) {
    Span span = executionAttributes.getAttribute(APPLICATION_SPAN);
    span.annotate("ws");
  }

  @Override public void afterTransmission(Context.AfterTransmission context,
      ExecutionAttributes executionAttributes) {
    Span span = executionAttributes.getAttribute(APPLICATION_SPAN);
    span.annotate("wr");
  }

  /**
   * After a SDK request has been executed
   */
  @Override public void afterExecution(
      Context.AfterExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span span = executionAttributes.getAttribute(APPLICATION_SPAN);
    handler.handleReceive(new HttpClientResponse(context.httpResponse()), null, span);
    span.finish();
  }

  /**
   * After a SDK request has failed
   */
  @Override public void onExecutionFailure(
      Context.FailedExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span span = executionAttributes.getAttribute(APPLICATION_SPAN);
    handler.handleReceive(null, context.exception(), span);
    span.error(context.exception());
    span.finish();
  }

  private String getAwsOperationNameFromRequestClass(SdkRequest request) {
    String operation = request.getClass().getSimpleName();
    if (operation.endsWith("Request")) {
      return operation.substring(0, operation.length() - 7);
    }
    return operation;
  }
}
