package brave.instrumentation.aws;

import brave.Span;
import brave.Tracing;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.handlers.RequestHandler2;
import zipkin2.Endpoint;

public class TracingRequestHandler extends RequestHandler2 {
  private static final HandlerContextKey<Span> SPAN = new HandlerContextKey<>(Span.class.getCanonicalName());

  @Override public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {
    return super.beforeExecution(request);
  }

  @Override public void beforeRequest(Request<?> request) {
    if (Tracing.currentTracer() == null) {
      return;
    }
    Span span = Tracing.currentTracer().nextSpan();
    span.start();
    span.remoteEndpoint(Endpoint.newBuilder().serviceName(request.getServiceName()).build());
    span.tag("aws.service_name", request.getServiceName());
    span.tag("aws.operation", getAwsOperationFromRequest(request));
    request.addHandlerContext(SPAN, span);
  }

  private String getAwsOperationFromRequest(Request<?> request) {
    // EX: ListBucketsRequest
    String operation = request.getOriginalRequest().getClass().getSimpleName();
    operation = operation.substring(0, operation.length() - 7); // Drop the "Request"
    return operation;
  }

  @Override public void afterAttempt(HandlerAfterAttemptContext context) {
    if (context.getException() != null) {
      Span span = context.getRequest().getHandlerContext(SPAN);
      if (span == null) {
        return;
      }
      span.error(context.getException());
    }
  }

  @Override public void afterResponse(Request<?> request, Response<?> response) {
    Span span = request.getHandlerContext(SPAN);
    if (span == null) {
      return;
    }
    tagSpanWithRequestId(span, response);
    span.finish();
  }

  @Override public void afterError(Request<?> request, Response<?> response, Exception e) {
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
    span.finish();
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
