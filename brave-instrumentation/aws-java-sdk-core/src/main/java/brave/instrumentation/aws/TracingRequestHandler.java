/**
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
import brave.Tracing;
import brave.http.HttpClientAdapter;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.handlers.RequestHandler2;

public class TracingRequestHandler extends RequestHandler2 {

  public static TracingRequestHandler create(Tracing tracing) {
    return new TracingRequestHandler(HttpTracing.create(tracing));
  }

  public static TracingRequestHandler create(HttpTracing httpTracing) {
    return new TracingRequestHandler(httpTracing);
  }

  private static final HttpClientAdapter<Request<?>, Response<?>> ADAPTER = new HttpAdapter();

  private static final HandlerContextKey<Span> SPAN = new HandlerContextKey<>(Span.class.getCanonicalName());
  private static final HandlerContextKey<TracingRequestHandler> TRACING_REQUEST_HANDLER_CONTEXT_KEY = new HandlerContextKey<>(TracingRequestHandler.class.getCanonicalName());

  private static final Propagation.Setter<Request<?>, String> SETTER = new Propagation.Setter<Request<?>, String>() {
    @Override public void put(Request<?> carrier, String key, String value) {
      carrier.addHeader(key, value);
    }
  };

  HttpClientHandler<Request<?>, Response<?>> handler;
  TraceContext.Injector<Request<?>> injector;

  TracingRequestHandler(HttpTracing httpTracing) {
    handler = HttpClientHandler.create(httpTracing, ADAPTER);
    injector = httpTracing.tracing().propagation().injector(SETTER);
  }

  @Override public void beforeRequest(Request<?> request) {
    if (requestIsAlreadyHandled(request)) {
      return;
    }
    Span span = handler.handleSend(injector, request);
    span.tag("aws.service_name", request.getServiceName());
    span.tag("aws.operation", getAwsOperationFromRequest(request));
    request.addHandlerContext(SPAN, span);
    request.addHandlerContext(TRACING_REQUEST_HANDLER_CONTEXT_KEY, this);
  }

  @Override public void afterAttempt(HandlerAfterAttemptContext context) {
    if (requestIsAlreadyHandled(context.getRequest())) {
      return;
    }
    if (context.getException() != null) {
      Span span = context.getRequest().getHandlerContext(SPAN);
      if (span == null) {
        return;
      }
      span.error(context.getException());
    }
  }

  @Override public void afterResponse(Request<?> request, Response<?> response) {
    if (requestIsAlreadyHandled(request)) {
      return;
    }
    Span span = request.getHandlerContext(SPAN);
    if (span == null) {
      return;
    }
    tagSpanWithRequestId(span, response);
    handler.handleReceive(response, null, span);
  }

  @Override public void afterError(Request<?> request, Response<?> response, Exception e) {
    if (requestIsAlreadyHandled(request)) {
      return;
    }
    Span span = request.getHandlerContext(SPAN);
    if (span == null) {
      return;
    }
    if (response != null) {
      tagSpanWithRequestId(span, response);
    } else if (e != null) {
      if (e instanceof AmazonServiceException) {
        tagSpanWithRequestId(span, (AmazonServiceException) e);
      }
    }
    handler.handleReceive(response, e, span);
  }


  private String getAwsOperationFromRequest(Request<?> request) {
    // EX: ListBucketsRequest
    String operation = request.getOriginalRequest().getClass().getSimpleName();
    operation = operation.substring(0, operation.length() - 7); // Drop the "Request"
    return operation;
  }

  private boolean requestIsAlreadyHandled(Request<?> request) {
    return request.getHandlerContext(TRACING_REQUEST_HANDLER_CONTEXT_KEY) != null &&
        request.getHandlerContext(TRACING_REQUEST_HANDLER_CONTEXT_KEY) != this;
  }

  private void tagSpanWithRequestId(Span span, Response response) {
    String requestId = null;
    if (response.getAwsResponse() instanceof AmazonWebServiceResult<?>) {
      ResponseMetadata metadata = ((AmazonWebServiceResult<?>) response.getAwsResponse()).getSdkResponseMetadata();
      if (null != metadata) {
        requestId = metadata.getRequestId();
      }
    } else if (response.getHttpResponse() != null) {
      if (response.getHttpResponse().getHeader("x-amz-request-id") != null) {
        requestId = response.getHttpResponse().getHeader("x-amz-request-id");
      }
    }
    if (requestId != null) {
      span.tag("aws.request_id", requestId);
    }
  }

  private void tagSpanWithRequestId(Span span, AmazonServiceException exception) {
    String requestId = null;
    if (exception.getRequestId() != null) {
      requestId = exception.getRequestId();
    }
    if (requestId != null) {
      span.tag("aws.request_id", requestId);
    }
  }
}
