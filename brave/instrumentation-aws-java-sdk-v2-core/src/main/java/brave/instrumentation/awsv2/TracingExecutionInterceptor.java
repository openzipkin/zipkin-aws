/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.instrumentation.awsv2.AwsSdkTracing.HttpClientRequest;
import brave.instrumentation.awsv2.AwsSdkTracing.HttpClientResponse;
import brave.internal.Nullable;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
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
  static final ExecutionAttribute<Span> SPAN = new ExecutionAttribute<>(Span.class.getName());

  final HttpClientHandler<brave.http.HttpClientRequest, brave.http.HttpClientResponse> handler;

  TracingExecutionInterceptor(HttpTracing httpTracing) {
    this.handler = HttpClientHandler.create(httpTracing);
  }

  /**
   * Before an individual http request is finalized. This is only called once per operation, meaning
   * we can only have one span per operation.
   */
  @Override public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context,
      ExecutionAttributes executionAttributes
  ) {
    HttpClientRequest request = new HttpClientRequest(context.httpRequest());

    Span span = handler.handleSend(request);
    executionAttributes.putAttribute(SPAN, span);

    String serviceName = executionAttributes.getAttribute(SdkExecutionAttribute.SERVICE_NAME);
    String operation = getAwsOperationNameFromRequestClass(context.request());
    // TODO: This overwrites user configuration. We don't do this in other layered tools such
    // as WebMVC. Instead, we add tags (such as we do here) and neither overwrite the name, nor
    // remoteServiceName. Users can always remap in an span handler using tags!
    span.name(operation)
        .remoteServiceName(serviceName)
        .tag("aws.service_name", serviceName)
        .tag("aws.operation", operation);

    return request.build();
  }

  /**
   * Before sending an http request. Will be called multiple times in the case of retries.
   */
  @Override public void beforeTransmission(Context.BeforeTransmission context,
      ExecutionAttributes executionAttributes) {
    Span span = executionAttributes.getAttribute(SPAN);
    if (span == null) {
      // An evil interceptor deleted our attribute.
      return;
    }
    span.annotate("ws");
  }

  /**
   * After sending an http request. Will be called multiple times in the case of retries.
   */
  @Override public void afterTransmission(Context.AfterTransmission context,
      ExecutionAttributes executionAttributes) {
    Span span = executionAttributes.getAttribute(SPAN);
    if (span == null) {
      // An evil interceptor deleted our attribute.
      return;
    }
    span.annotate("wr");
  }

  /**
   * After a SDK request has been executed
   */
  @Override public void afterExecution(
      Context.AfterExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span span = executionAttributes.getAttribute(SPAN);
    if (span == null) {
      // An evil interceptor deleted our attribute.
      return;
    }
    handler.handleReceive(
        new HttpClientResponse(context.httpRequest(), context.httpResponse(), null), span);
  }

  /**
   * After a SDK request has failed
   */
  @Override public void onExecutionFailure(
      Context.FailedExecution context,
      ExecutionAttributes executionAttributes
  ) {
    Span span = executionAttributes.getAttribute(SPAN);
    if (span == null) {
      // An evil interceptor deleted our attribute.
      return;
    }
    handler.handleReceive(new HttpClientResponse(
        context.httpRequest().orElse(null),
        context.httpResponse().orElse(null),
        maybeError(context)), span);
  }

  /**
   * Returns {@code null} when there's neither a cause nor an AWS error message. This assumes it was
   * a plain HTTP status failure.
   */
  @Nullable static Throwable maybeError(Context.FailedExecution context) {
    Throwable error = context.exception();
    if (error.getCause() == null && error instanceof AwsServiceException) {
      AwsServiceException serviceException = (AwsServiceException) error;
      if (serviceException.awsErrorDetails().errorMessage() == null) {
        return null;
      }
    }
    return error;
  }

  static String getAwsOperationNameFromRequestClass(SdkRequest request) {
    String operation = request.getClass().getSimpleName();
    if (operation.endsWith("Request")) {
      return operation.substring(0, operation.length() - 7);
    }
    return operation;
  }
}
