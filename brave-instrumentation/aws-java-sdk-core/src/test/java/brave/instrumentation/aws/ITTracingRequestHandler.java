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

import brave.ScopedSpan;
import brave.SpanCustomizer;
import brave.Tracer;
import brave.http.HttpAdapter;
import brave.http.HttpClientParser;
import brave.http.HttpTracing;
import brave.internal.HexCodec;
import brave.sampler.Sampler;
import brave.test.http.ITHttpAsyncClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.Test;
import zipkin2.Span;

import static java.util.Arrays.asList;
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
        .withRequestHandlers(new TracingRequestHandler(httpTracing))
        .withClientConfiguration(clientConfiguration)
        .build();
  }

  @Override protected void closeClient(AmazonDynamoDB dynamoDB) throws Exception {
    dynamoDB.shutdown();
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
    String uri = "/"; // Always '/'
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

    List<Span> spans = Arrays.asList(takeSpan(), takeSpan());
    spans.stream().filter(s -> s.parentId() != null).forEach(s -> {
      assertThat(s.remoteServiceName())
          .isEqualTo("amazondynamodbv2");
      assertThat(s.name()).isEqualTo("getitem");

      assertThat(s.tags())
          .containsEntry("http.url", url(uri))
          .containsEntry("request_customizer.is_span", "false")
          .containsEntry("response_customizer.is_span", "false");
    });
  }

  /*
   * Tests overriden due to the multi-span implementation of the client, application and client
   */

  /** Modified to check hierarchy from parent->application->client */
  @Override public void makesChildOfCurrentSpan() throws Exception {
    Tracer tracer = httpTracing.tracing().tracer();
    server.enqueue(new MockResponse());

    ScopedSpan parent = tracer.startScopedSpan("test");
    try {
      get(client, "/foo");
    } finally {
      parent.finish();
    }

    List<Span> spans = Arrays.asList(takeSpan(), takeSpan(), takeSpan());
    Span applicationSpan = spans.stream().filter(s -> s.parentId().equals(HexCodec.toLowerHex(parent.context().spanId()))).findFirst().get();
    Span clientSpan = spans.stream().filter(s -> s.parentId().equals(applicationSpan.id())).findFirst().get();

    RecordedRequest request = server.takeRequest();
    assertThat(request.getHeader("x-b3-traceId"))
        .isEqualTo(parent.context().traceIdString());
    assertThat(request.getHeader("x-b3-parentspanid"))
        .isEqualTo(applicationSpan.id());
    assertThat(request.getHeader("x-b3-spanid"))
        .isEqualTo(clientSpan.id());

    assertThat(spans)
        .extracting(Span::kind)
        .containsOnly(null, Span.Kind.CLIENT, null);
  }

  @Override public void propagatesSpan() throws Exception {
    super.propagatesSpan();
    takeSpan();
  }

  @Override public void reportsSpanOnTransportException() throws Exception {
    getWithRetries(); // Our new impl of this method
  }

  @Override public void propagates_sampledFalse() throws Exception {
    close();
    httpTracing = HttpTracing.create(tracingBuilder(Sampler.NEVER_SAMPLE).build());
    client = newClient(server.getPort());

    server.enqueue(new MockResponse());
    get(client, "/foo");

    RecordedRequest request = server.takeRequest();
    assertThat(request.getHeaders().toMultimap())
        .containsKeys("x-b3-traceId", "x-b3-spanId")
        .containsEntry("x-b3-sampled", asList("0"));
  }

  @Override public void addsErrorTagOnTransportException() throws Exception {
    List<Span> spans = getWithRetries();
    for (Span span : spans) {
      assertThat(span.tags())
          .containsKey("error");
    }
  }

  @Test public void retriesAreChildrenOfApplicationSpan() throws Exception {
    List<Span> spans = getWithRetries();
    assertThat(spans.stream().anyMatch(s -> s.parentId() == null)).isTrue();
    Span parent = spans.stream().filter(s -> s.parentId() == null).findFirst().get();
    assertThat(parent.kind()).isNull();

    spans.stream().filter(s -> s.parentId() != null).forEach(s -> {
      assertThat(s.parentId()).isEqualTo(parent.id());
      assertThat(s.kind()).isEqualTo(Span.Kind.CLIENT);
    });
  }

  private List<Span> getWithRetries() throws InterruptedException {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));

    try {
      get(client, "");
    } catch (Exception e) {
      // Span will have error
    }

    return new ArrayList<>(Arrays.asList(takeSpan(), takeSpan(), takeSpan(), takeSpan()));
  }

  /** Body's inherently have a structure, and we use the operation name as the span name */
  @Override public void post() throws Exception {
    String path = "table";
    String body = "{\"TableName\":\"table\",\"Key\":{}}";
    server.enqueue(new MockResponse());

    post(client, path, body);

    assertThat(server.takeRequest().getBody().readUtf8())
        .isEqualTo(body);

    Span span = takeSpan();
    assertThat(span.name())
        .isEqualTo("getitem");
    assertThat(span.remoteServiceName())
        .isEqualTo("amazondynamodbv2");

    takeSpan();
  }

  @Override public void propagatesExtra_newTrace() throws Exception {
    super.propagatesExtra_newTrace();
    takeSpan();
  }

  @Override public void reportsClientKindToZipkin() throws Exception {
    super.reportsClientKindToZipkin();
    takeSpan();
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
