package brave.instrumentation.aws;

import brave.Span;
import brave.Tracing;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.handlers.RequestHandler2;

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
    request.addHandlerContext(SPAN, span);
  }

  @Override public void afterResponse(Request<?> request, Response<?> response) {
    Span span = request.getHandlerContext(SPAN);
    if (span == null) {
      return;
    }
    span.finish();
  }

  @Override public void afterError(Request<?> request, Response<?> response, Exception e) {
    Span span = request.getHandlerContext(SPAN);
    if (span == null) {
      return;
    }
    span.error(e);
    span.finish();
  }
}
