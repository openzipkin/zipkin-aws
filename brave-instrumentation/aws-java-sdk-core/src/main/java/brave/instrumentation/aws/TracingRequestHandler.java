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
package brave.instrumentation.aws;

import brave.Span;
import brave.Tracer;
import brave.http.HttpClientAdapter;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.propagation.Propagation;
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
 * name of the operation being done. If the request results in an error then the span will be
 * tagged with the error. The AWS request ID is added when available.
 */
final class TracingRequestHandler extends RequestHandler2 {
  static final HandlerContextKey<Span> APPLICATION_SPAN =
      new HandlerContextKey<>("APPLICATION_SPAN");
  static final HandlerContextKey<TraceContext> DEFERRED_ROOT_CONTEXT =
      new HandlerContextKey<>("DEFERRED_ROOT_CONTEXT");
  static final HandlerContextKey<Span> CLIENT_SPAN =
      new HandlerContextKey<>(Span.class.getCanonicalName());

  static final HttpClientAdapter<Request<?>, Response<?>> ADAPTER = new HttpAdapter();

  static final Propagation.Setter<Request<?>, String> SETTER =
      new Propagation.Setter<Request<?>, String>() {
        @Override public void put(Request<?> carrier, String key, String value) {
          carrier.addHeader(key, value);
        }
      };

  final HttpTracing httpTracing;
  final Tracer tracer;
  final HttpClientHandler<Request<?>, Response<?>> handler;
  final TraceContext.Injector<Request<?>> injector;

  TracingRequestHandler(HttpTracing httpTracing) {
    this.httpTracing = httpTracing;
    this.tracer = httpTracing.tracing().tracer();
    this.injector = httpTracing.tracing().propagation().injector(SETTER);
    this.handler = HttpClientHandler.create(httpTracing, ADAPTER);
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
    TraceContext deferredRootContext = context.getRequest().getHandlerContext(DEFERRED_ROOT_CONTEXT);
    Span applicationSpan;
    if (deferredRootContext != null) {
      Boolean sampled = httpTracing.clientSampler().trySample(ADAPTER, context.getRequest());
      if (sampled == null) {
        sampled = httpTracing.tracing().sampler().isSampled(deferredRootContext.traceId());
      }
      applicationSpan = tracer.toSpan(deferredRootContext.toBuilder().sampled(sampled).build());
      context.getRequest().addHandlerContext(APPLICATION_SPAN, applicationSpan.start());
    } else {
      applicationSpan = context.getRequest().getHandlerContext(APPLICATION_SPAN);
    }
    String operation = getAwsOperationFromRequest(context.getRequest());
    applicationSpan.name("aws-sdk")
        .tag("aws.service_name", context.getRequest().getServiceName())
        .tag("aws.operation", operation);
    Span clientSpan = nextClientSpan(context.getRequest(), applicationSpan, operation);
    context.getRequest().addHandlerContext(CLIENT_SPAN, clientSpan);
  }

  @Override public final void afterAttempt(HandlerAfterAttemptContext context) {
    Span clientSpan = context.getRequest().getHandlerContext(CLIENT_SPAN);
    if (context.getException() != null
        && context.getException() instanceof AmazonServiceException) {
      tagSpanWithRequestId(clientSpan, (AmazonServiceException) context.getException());
    } else {
      tagSpanWithRequestId(clientSpan, context.getResponse());
    }
    handler.handleReceive(context.getResponse(), context.getException(), clientSpan);
  }

  @Override public final void afterResponse(Request<?> request, Response<?> response) {
    Span applicationSpan = request.getHandlerContext(APPLICATION_SPAN);
    applicationSpan.finish();
  }

  @Override public final void afterError(Request<?> request, Response<?> response, Exception e) {
    Span applicationSpan = request.getHandlerContext(APPLICATION_SPAN);
    applicationSpan.error(e);
    applicationSpan.finish();
  }

  private Span nextClientSpan(Request<?> request, Span applicationSpan, String operation) {
    Span span = tracer.newChild(applicationSpan.context());
    handler.handleSend(injector, request, span);
    return span.name(operation)
        .remoteServiceName(request.getServiceName());
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
