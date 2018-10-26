package brave.instrumentation.aws;

import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.http.HttpTracing;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.DefaultRequest;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingRequestHandlerTest {
  private BlockingQueue<Span> spans = new LinkedBlockingQueue<>();

  Tracing tracing;
  TracingRequestHandler handler;

  @Before
  public void setup() {
    tracing = tracingBuilder().build();
    handler = new TracingRequestHandler(HttpTracing.create(tracing));
  }

  @After
  public void cleanup() {
    tracing.close();
  }

  @Test
  public void handlesAmazonServiceExceptions() throws Exception {
    brave.Span braveSpan = tracing.tracer().nextSpan();
    AmazonServiceException exception = new ProvisionedThroughputExceededException("test");
    exception.setRequestId("abcd");

    DefaultRequest request = new DefaultRequest("test");
    request.addHandlerContext(new HandlerContextKey<>(brave.Span.class.getCanonicalName()), braveSpan);
    HandlerAfterAttemptContext context = HandlerAfterAttemptContext.builder()
        .withRequest(request)
        .withException(exception)
        .build();

    handler.afterAttempt(context);
    Span reportedSpan = spans.take();
    assertThat(reportedSpan.traceId()).isEqualToIgnoringCase(braveSpan.context().traceIdString());
    assertThat(reportedSpan.tags()).containsKey("error");
    assertThat(reportedSpan.tags().get("aws.request_id")).isEqualToIgnoringCase("abcd");

    assertThat(spans.isEmpty()).isTrue();
  }

  private Tracing.Builder tracingBuilder() {
    return Tracing.newBuilder()
        .spanReporter(spans::add)
        .currentTraceContext(
            ThreadLocalCurrentTraceContext.newBuilder()
                .addScopeDecorator(ThreadContextScopeDecorator.create()) // connect to log4j
                .addScopeDecorator(StrictScopeDecorator.create())
                .build())
        .sampler(Sampler.ALWAYS_SAMPLE);
  }
}
