package brave.instrumentation.aws;

import brave.Tracing;
import brave.context.log4j2.ThreadContextCurrentTraceContext;
import brave.propagation.StrictCurrentTraceContext;
import brave.sampler.Sampler;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class CurrentTracingRequestHandlerTest {

  @Rule
  public MockDynamoDBServer dynamoDBServer = new MockDynamoDBServer();

  BlockingQueue<Span> spans = new LinkedBlockingQueue<>();
  AmazonDynamoDB client = clientBuilder();

  @Before
  public void setup() {
    tracingBuilder().build();
  }

  // See brave.http.ITHttp for rationale on polling after tests complete
  @Rule public TestRule assertSpansEmpty = new TestWatcher() {
    // only check success path to avoid masking assertion errors or exceptions
    @Override protected void succeeded(Description description) {
      try {
        assertThat(spans.poll(100, TimeUnit.MILLISECONDS))
            .withFailMessage("Span remaining in queue. Check for redundant reporting")
            .isNull();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  };

  @Test
  public void test() throws InterruptedException {
    dynamoDBServer.enqueue(new MockResponse().setBody("{\"LastEvaluatedTableName\": \"Thread\",\"TableNames\": [\"Forum\",\"Reply\",\"Thread\"]}"));

    client.deleteItem("test", Collections.EMPTY_MAP);

    Span span = spans.take();
    assertThat(span.remoteServiceName()).isEqualToIgnoringCase("amazondynamodbv2");
    assertThat(span.tags().get("aws.operation")).isEqualToIgnoringCase("deleteitem");
  }

  private Tracing.Builder tracingBuilder() {
    return Tracing.newBuilder()
        .spanReporter(spans::add)
        .currentTraceContext( // connect to log4j
            ThreadContextCurrentTraceContext.create(new StrictCurrentTraceContext()))
        .sampler(Sampler.ALWAYS_SAMPLE);
  }

  private AmazonDynamoDB clientBuilder() {
    return AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoDBServer.url(), "us-east-1"))
        .withRequestHandlers(new CurrentTracingRequestHandler())
        .build();
  }
}
