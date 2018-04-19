package brave.instrumentation.aws;

import brave.Tracing;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingRequestHandlerTest extends CurrentTracingRequestHandlerTest {
  private TracingRequestHandler tracingRequestHandler;
  private AmazonDynamoDB client;

  @Before
  public void setup() {
    Tracing tracing = tracingBuilder().build();
    tracingRequestHandler = TracingRequestHandler.builder().tracer(tracing.tracer()).build();
    client = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret")))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoDBServer.url(), "us-east-1"))
        .withRequestHandlers(tracingRequestHandler, new CurrentTracingRequestHandler())
        .build();
  }

  @Test
  public void testThatOnlyOneHandlerRuns() throws InterruptedException {
    dynamoDBServer.enqueue(createDeleteItemResponse());

    client = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret")))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoDBServer.url(), "us-east-1"))
        .withRequestHandlers(new CurrentTracingRequestHandler(), tracingRequestHandler, new CurrentTracingRequestHandler())
        .build();

    client.deleteItem("test", Collections.singletonMap("key", new AttributeValue("value")));

    spans.poll(100, TimeUnit.MILLISECONDS);
    // Let the test rule verify no spans are remaining
  }

  @Override protected AmazonDynamoDB client() {
    return client;
  }
}
