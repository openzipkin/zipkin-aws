/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
import brave.http.HttpClientAdapter;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

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
 * name of the operation being done. If the request results in an error then the span will be
 * tagged with the error. The AWS request ID is added when available.
 */
public class TracingExecutionInterceptor implements ExecutionInterceptor {
  static final ExecutionAttribute<TraceContext> DEFERRED_ROOT_CONTEXT =
      new ExecutionAttribute<>("DEFERRED_ROOT_CONTEXT");
  static final ExecutionAttribute<Span> APPLICATION_SPAN =
      new ExecutionAttribute<>("APPLICATION_SPAN");
  static final ExecutionAttribute<Span> CLIENT_SPAN =
      new ExecutionAttribute<>(Span.class.getCanonicalName());

  static final ExecutionAttribute<String> SERVICE_NAME = SdkExecutionAttribute.SERVICE_NAME;

  static final HttpClientAdapter<SdkHttpRequest.Builder, SdkHttpResponse> ADAPTER =
      new HttpAdapter();
  static final Propagation.Setter<SdkHttpRequest.Builder, String> SETTER =
      SdkHttpRequest.Builder::appendHeader;

  final Tracer tracer;
  final HttpTracing httpTracing;
  final HttpClientHandler<SdkHttpRequest.Builder, SdkHttpResponse> handler;
  final TraceContext.Injector<SdkHttpRequest.Builder> injector;

  TracingExecutionInterceptor(HttpTracing httpTracing) {
    this.httpTracing = httpTracing;
    this.tracer = httpTracing.tracing().tracer();
    this.injector = httpTracing.tracing().propagation().injector(SETTER);
    this.handler = HttpClientHandler.create(httpTracing, ADAPTER);
  }

  /**
   * Before the SDK request leaves the calling thread
   */
  @Override public void beforeExecution(
      Context.BeforeExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span maybeDeferredRootSpan = tracer.nextSpan();
    if (maybeDeferredRootSpan.context().parentIdAsLong() == 0) { // Deferred to sampling when we have http context
      // When we rebuild the context in the 2nd put, we lose the reference here which is the one
      // inserted into the PendingSpans list, so we need to save it to keep the span finishable
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
    Span applicationSpan;
    if (maybeDeferredRootSpan != null) {
      Boolean sampled = httpTracing.clientSampler().trySample(
          ADAPTER,
          context.httpRequest().toBuilder()
      );
      if (sampled == null) {
        sampled = httpTracing.tracing().sampler().isSampled(maybeDeferredRootSpan.traceId());
      }
      applicationSpan = tracer.toSpan(maybeDeferredRootSpan.toBuilder().sampled(sampled).build());
      executionAttributes.putAttribute(APPLICATION_SPAN, applicationSpan.start());
    } else {
      applicationSpan = executionAttributes.getAttribute(APPLICATION_SPAN);
    }
    String serviceName = executionAttributes.getAttribute(SERVICE_NAME);
    String operation = getAwsOperationNameFromRequestClass(context.request());
    applicationSpan.name("aws-sdk")
        .tag("aws.service_name", serviceName)
        .tag("aws.operation", operation);

    return context.httpRequest().copy(builder -> {
      Span clientSpan = nextClientSpan(applicationSpan, serviceName, operation, builder);
      executionAttributes.putAttribute(CLIENT_SPAN, clientSpan);
    });
  }

  /**
   * After individual http response, may run multiple times if retries occur
   */
  @Override
  public void beforeUnmarshalling(
      Context.BeforeUnmarshalling context,
      ExecutionAttributes executionAttributes
  ) {
    Span clientSpan = executionAttributes.getAttribute(CLIENT_SPAN);
    if (!context.httpResponse().isSuccessful()) {
      clientSpan.tag("error", context.httpResponse().statusText()
          .orElse("Unknown AWS service error"));
    }
    handler.handleReceive(context.httpResponse(), null, clientSpan);
    executionAttributes.putAttribute(CLIENT_SPAN, null);
  }



  /**
   * After a SDK request has been executed
   */
  @Override public void afterExecution(
      Context.AfterExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span applicationSpan = executionAttributes.getAttribute(APPLICATION_SPAN);
    applicationSpan.finish();
  }

  /**
   * After a SDK request has failed
   */
  @Override public void onExecutionFailure(
      Context.FailedExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span applicationSpan = executionAttributes.getAttribute(APPLICATION_SPAN);
    applicationSpan.error(context.exception());
  }

  private Span nextClientSpan(
      Span applicationSpan,
      String serviceName,
      String operation,
      SdkHttpRequest.Builder sdkHttpRequestBuilder
  ) {
    Span span = tracer.newChild(applicationSpan.context());
    handler.handleSend(injector, sdkHttpRequestBuilder, span);
    return span.name(operation)
        .remoteServiceName(serviceName);
  }

  private String getAwsOperationNameFromRequestClass(SdkRequest request) {
    String operation = request.getClass().getSimpleName();
    if (operation.endsWith("Request")) {
      return operation.substring(0, operation.length() - 7);
    }
    return operation;
  }
}
