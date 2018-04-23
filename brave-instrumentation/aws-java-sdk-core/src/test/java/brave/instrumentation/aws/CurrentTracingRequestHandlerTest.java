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

import brave.Tracing;
import brave.context.log4j2.ThreadContextCurrentTraceContext;
import brave.propagation.StrictCurrentTraceContext;
import brave.sampler.Sampler;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import org.junit.After;
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
  private AmazonDynamoDB client;

  @Before
  public void setup() {
    tracingBuilder().build();
    client = clientBuilder();
  }

  @After
  public void cleanup() {
    Tracing.current().close();
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
  public void testSpanCreatedAndTagsApplied() throws InterruptedException {
    dynamoDBServer.enqueue(createDeleteItemResponse());

    client().deleteItem("test", Collections.singletonMap("key", new AttributeValue("value")));

    Span span = spans.poll(100, TimeUnit.MILLISECONDS);
    assertThat(span.remoteServiceName()).isEqualToIgnoringCase("amazondynamodbv2");
    assertThat(span.tags().get("aws.operation")).isEqualToIgnoringCase("deleteitem");
    assertThat(span.tags().get("aws.request_id")).isEqualToIgnoringCase("abcd");
  }

  @Test
  public void testTracingCreatedAfterRequestHandler() throws InterruptedException {
    dynamoDBServer.enqueue(createDeleteItemResponse());
    dynamoDBServer.enqueue(createDeleteItemResponse());
    Tracing.current().close();

    client().deleteItem("test", Collections.singletonMap("key", new AttributeValue("value")));

    Span span = spans.poll(100, TimeUnit.MILLISECONDS);
    assertThat(span).isNull();

    tracingBuilder().build();
    client().deleteItem("test", Collections.singletonMap("key", new AttributeValue("value")));

    span = spans.poll(100, TimeUnit.MILLISECONDS);
    assertThat(span).isNotNull();
  }

  MockResponse createDeleteItemResponse() {
    MockResponse response = new MockResponse();
    response.setBody("{}");
    response.addHeader("x-amzn-RequestId","abcd");
    return response;
  }

  Tracing.Builder tracingBuilder() {
    return Tracing.newBuilder()
        .spanReporter(spans::add)
        .currentTraceContext( // connect to log4j
            ThreadContextCurrentTraceContext.create(new StrictCurrentTraceContext()))
        .sampler(Sampler.ALWAYS_SAMPLE);
  }

  private AmazonDynamoDB clientBuilder() {
    return AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret")))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoDBServer.url(), "us-east-1"))
        .withRequestHandlers(new CurrentTracingRequestHandler())
        .build();
  }

  protected AmazonDynamoDB client() {
    return client;
  }
}
