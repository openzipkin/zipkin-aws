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

import brave.SpanCustomizer;
import brave.http.HttpAdapter;
import brave.http.HttpClientParser;
import brave.test.http.ITHttpAsyncClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import java.util.Collections;
import okhttp3.mockwebserver.MockResponse;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class ITTracingRequestHandler extends ITHttpAsyncClient<AmazonDynamoDB> {
  @Override protected void getAsync(AmazonDynamoDB dynamoDB, String s) throws Exception {
    dynamoDB.getItem(s, Collections.EMPTY_MAP);
  }

  @Override protected AmazonDynamoDB newClient(int i) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setMaxErrorRetry(2);
    clientConfiguration.setRequestTimeout(1000);

    return AmazonDynamoDBAsyncClientBuilder.standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:" + i, "us-east-1"))
        .withRequestHandlers(TracingRequestHandler.create(httpTracing))
        .withClientConfiguration(clientConfiguration)
        .build();
  }

  @Override protected void closeClient(AmazonDynamoDB dynamoDB) throws Exception {

  }

  @Override protected void get(AmazonDynamoDB dynamoDB, String s) throws Exception {
    dynamoDB.getItem(s, Collections.EMPTY_MAP);
  }

  @Override protected void post(AmazonDynamoDB dynamoDB, String s, String s1) throws Exception {
    dynamoDB.getItem(s, Collections.EMPTY_MAP);
  }

  /*
   * Tests overridden due to RPC nature of AWS API
   */

  /** Span name doesn't conform to expectation */
  @Override public void supportsPortableCustomization() throws Exception {
    String uri = "";
    close();
    httpTracing = httpTracing.toBuilder()
        .clientParser(new HttpClientParser() {
          @Override
          public <Req> void request(brave.http.HttpAdapter<Req, ?> adapter, Req req,
              SpanCustomizer customizer) {
            customizer.tag("http.url", adapter.url(req)); // just the path is logged by default
            customizer.tag("request_customizer.is_span", (customizer instanceof brave.Span) + "");
          }

          @Override
          public <Resp> void response(HttpAdapter<?, Resp> adapter, Resp res, Throwable error,
              SpanCustomizer customizer) {
            super.response(adapter, res, error, customizer);
            customizer.tag("response_customizer.is_span", (customizer instanceof brave.Span) + "");
          }
        })
        .build().clientOf("remote-service");

    client = newClient(server.getPort());
    server.enqueue(new MockResponse());
    get(client, uri);

    Span span = takeSpan();

    assertThat(span.remoteServiceName())
        .isEqualTo("amazondynamodbv2");

    assertThat(span.tags())
        .containsEntry("http.url", url(uri))
        .containsEntry("request_customizer.is_span", "false")
        .containsEntry("response_customizer.is_span", "false");
  }

  /** Body's inherently have a structure */
  @Override public void post() throws Exception {
    String path = "table";
    String body = "{\"TableName\":\"table\",\"Key\":{}}";
    server.enqueue(new MockResponse());

    post(client, path, body);

    assertThat(server.takeRequest().getBody().readUtf8())
        .isEqualTo(body);

    Span span = takeSpan();
    assertThat(span.name())
        .isEqualTo("post");
  }

  /*
   * Tests that don't work
   */

  /** AWS doesn't use redirect */
  @Override public void redirect() {}

  /** Paths don't conform to expectation, always / */
  @Override public void customSampler() {}

  /** Unable to parse remote endpoint IP, would require DNS lookup */
  @Override public void reportsServerAddress() {}

  /** Path is always empty */
  @Override public void httpPathTagExcludesQueryParams() {}

  /** Error has exception instead of status code */
  @Override public void addsStatusCodeWhenNotOk() {}

  /** All http methods are POST */
  @Override public void defaultSpanNameIsMethodName() {}
}
