/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.aws;

import brave.Span;
import brave.Tracer;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.instrumentation.aws.AwsClientTracing.HttpClientRequest;
import brave.instrumentation.aws.AwsClientTracing.HttpClientResponse;
import brave.propagation.TraceContext;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerBeforeAttemptContext;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.handlers.RequestHandler2;

/**
 * Traces AWS Java SDK calls. Adds on the standard zipkin/brave http tags, as well as tags that
 * align with the XRay data model.
 *
 * This implementation creates 2 types of spans to allow for better error visibility.
 *
 * The outer span, "Application Span", wraps the whole SDK operation. This span uses aws-sdk as it's
 * name and will NOT have a remoteService configuration, making it a local span. If the entire
 * operation results in an error then this span will have an error tag with the cause.
 *
 * The inner span, "Client Span", is created for each outgoing HTTP request. This span will be of
 * type CLIENT. The remoteService will be the name of the AWS service, and the span name will be the
 * name of the operation being done. If the request results in an error then the span will be tagged
 * with the error. The AWS request ID is added when available.
 */
final class TracingRequestHandler extends RequestHandler2 {
  static final HandlerContextKey<Span> APPLICATION_SPAN =
      new HandlerContextKey<>("APPLICATION_SPAN");
  static final HandlerContextKey<TraceContext> DEFERRED_ROOT_CONTEXT =
      new HandlerContextKey<>("DEFERRED_ROOT_CONTEXT");
  static final HandlerContextKey<Span> CLIENT_SPAN =
      new HandlerContextKey<>(Span.class.getCanonicalName());

  final HttpTracing httpTracing;
  final Tracer tracer;
  final HttpClientHandler<brave.http.HttpClientRequest, brave.http.HttpClientResponse> handler;

  TracingRequestHandler(HttpTracing httpTracing) {
    this.httpTracing = httpTracing;
    this.tracer = httpTracing.tracing().tracer();
    this.handler = HttpClientHandler.create(httpTracing);
  }

  @Override public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {
    Span applicationSpan = tracer.nextSpan();
    // new root span, but we don't yet know if we should sample it
    if (applicationSpan.context().parentIdAsLong() == 0) {
      request.addHandlerContext(DEFERRED_ROOT_CONTEXT, applicationSpan.context());
    } else {
      request.addHandlerContext(APPLICATION_SPAN, applicationSpan.start());
    }
    return request;
  }

  @Override public void beforeAttempt(HandlerBeforeAttemptContext context) {
    TraceContext deferredRootContext =
        context.getRequest().getHandlerContext(DEFERRED_ROOT_CONTEXT);
    Span applicationSpan;
    HttpClientRequest request = new HttpClientRequest(context.getRequest());
    if (deferredRootContext != null) {
      Boolean sampled = httpTracing.clientRequestSampler().trySample(request);
      if (sampled == null) {
        sampled = httpTracing.tracing().sampler().isSampled(deferredRootContext.traceId());
      }
      applicationSpan = tracer.toSpan(deferredRootContext.toBuilder().sampled(sampled).build());
      context.getRequest().addHandlerContext(APPLICATION_SPAN, applicationSpan.start());
    } else {
      applicationSpan = context.getRequest().getHandlerContext(APPLICATION_SPAN);
    }

    if (applicationSpan == null) {
      return;
    }

    String operation = getAwsOperationFromRequest(context.getRequest());
    applicationSpan.name("aws-sdk")
        .tag("aws.service_name", context.getRequest().getServiceName())
        .tag("aws.operation", operation);

    Span span = tracer.newChild(applicationSpan.context());
    handler.handleSend(request, span);
    span.name(operation).remoteServiceName(context.getRequest().getServiceName());

    context.getRequest().addHandlerContext(CLIENT_SPAN, span);
  }

  @Override public final void afterAttempt(HandlerAfterAttemptContext context) {
    Span clientSpan = context.getRequest().getHandlerContext(CLIENT_SPAN);
    if (clientSpan == null) {
      return;
    }
    if (context.getException() instanceof AmazonServiceException) {
      tagSpanWithRequestId(clientSpan, (AmazonServiceException) context.getException());
    } else {
      tagSpanWithRequestId(clientSpan, context.getResponse());
    }
    handler.handleReceive(new HttpClientResponse(context), clientSpan);
  }

  @Override public final void afterResponse(Request<?> request, Response<?> response) {
    Span applicationSpan = request.getHandlerContext(APPLICATION_SPAN);
    if (applicationSpan != null) {
      applicationSpan.finish();
    }
  }

  @Override public final void afterError(Request<?> request, Response<?> response, Exception e) {
    Span applicationSpan = request.getHandlerContext(APPLICATION_SPAN);
    if (applicationSpan != null) {
      applicationSpan.error(e);
      applicationSpan.finish();
    }
  }

  private String getAwsOperationFromRequest(Request<?> request) {
    // EX: ListBucketsRequest
    String operation = request.getOriginalRequest().getClass().getSimpleName();
    if (operation.endsWith("Request")) {
      return operation.substring(0, operation.length() - 7);
    }
    return operation;
  }

  static void tagSpanWithRequestId(Span span, Response response) {
    String requestId = null;
    if (response != null) {
      if (response.getAwsResponse() instanceof AmazonWebServiceResult<?>) {
        ResponseMetadata metadata =
            ((AmazonWebServiceResult<?>) response.getAwsResponse()).getSdkResponseMetadata();
        if (null != metadata) {
          requestId = metadata.getRequestId();
        }
      } else if (response.getHttpResponse() != null) {
        if (response.getHttpResponse().getHeader("x-amz-request-id") != null) {
          requestId = response.getHttpResponse().getHeader("x-amz-request-id");
        }
      }
    }
    if (requestId != null) {
      span.tag("aws.request_id", requestId);
    }
  }

  static void tagSpanWithRequestId(Span span, AmazonServiceException exception) {
    String requestId = exception.getRequestId();
    if (requestId != null) {
      span.tag("aws.request_id", requestId);
    }
  }
}
