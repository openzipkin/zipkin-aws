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
  private AmazonDynamoDB client;

  @Before
  @Override public void setup() {
    Tracing tracing = tracingBuilder().build();
    TracingRequestHandler tracingRequestHandler = TracingRequestHandler.create(tracing);
    client = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret")))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoDBServer.url(), "us-east-1"))
        .withRequestHandlers(tracingRequestHandler, new CurrentTracingRequestHandler())
        .build();
  }

  @Test
  public void testThatOnlyOneHandlerRuns() throws InterruptedException {
    dynamoDBServer.enqueue(createDeleteItemResponse());

    client().deleteItem("test", Collections.singletonMap("key", new AttributeValue("value")));

    spans.poll(100, TimeUnit.MILLISECONDS);
    // Let the test rule verify no spans are remaining
  }

  @Override protected AmazonDynamoDB client() {
    return client;
  }
}
