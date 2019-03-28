/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package brave.instrumentation.awsv2;

import brave.ScopedSpan;
import brave.SpanCustomizer;
import brave.Tracer;
import brave.http.HttpAdapter;
import brave.http.HttpClientParser;
import brave.internal.HexCodec;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import brave.test.http.ITHttpClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import zipkin2.Span;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ITTracingExecutionInterceptor extends ITHttpClient<DynamoDbClient> {

  @Override protected DynamoDbClient newClient(int i) {
    ClientOverrideConfiguration configuration = ClientOverrideConfiguration.builder()
        .retryPolicy(RetryPolicy.builder().numRetries(3).build())
        .apiCallTimeout(Duration.ofMillis(100))
        .addExecutionInterceptor(AwsSdkTracing.create(httpTracing).executionInterceptor())
        .build();

    return client = DynamoDbClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .httpClient(primeHttpClient())
        .region(Region.US_EAST_1)
        .overrideConfiguration(configuration)
        .endpointOverride(URI.create("http://127.0.0.1:" + i))
        .build();
  }

  // For some reason, the first request will fail. This primes the connection until we know why
  SdkHttpClient primeHttpClient() {
    server.enqueue(new MockResponse());
    SdkHttpClient httpClient = UrlConnectionHttpClient.builder().build();
    try {
      httpClient.prepareRequest(HttpExecuteRequest.builder()
          .request(SdkHttpRequest.builder()
              .method(SdkHttpMethod.GET)
              .uri(server.url("/").uri())
              .build())
          .build()).call();
    } catch (IOException e) {
    } finally {
      try {
        server.takeRequest(1, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ex) {
      }
    }
    return httpClient;
  }

  @Override protected void closeClient(DynamoDbClient client) {
    client.close();
  }

  @Override protected void get(DynamoDbClient client, String s) {
    client.getItem(
        GetItemRequest.builder().tableName(s).key(Collections.emptyMap()).build());
  }

  @Override protected void post(DynamoDbClient client, String s, String s1) {
    client.putItem(
        PutItemRequest.builder().tableName(s).item(Collections.emptyMap()).build());
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

    List<Span> spans = asList(takeSpan(), takeSpan());
    spans.stream().filter(s -> s.parentId() != null).forEach(s -> {
      assertThat(s.remoteServiceName())
          .isEqualTo("dynamodb");
      assertThat(s.name()).isEqualTo("getitem");

      assertThat(s.tags())
          .containsEntry("http.url", url(uri))
          .containsEntry("request_customizer.is_span", "false")
          .containsEntry("response_customizer.is_span", "false");
    });
  }

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

    List<Span> spans = asList(takeSpan(), takeSpan(), takeSpan());
    Span applicationSpan = spans.stream()
        .filter(s -> HexCodec.toLowerHex(parent.context().spanId()).equals(s.parentId()))
        .findFirst()
        .get();
    Span clientSpan =
        spans.stream().filter(s -> applicationSpan.id().equals(s.parentId())).findFirst().get();

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

  @Override @Test public void propagates_sampledFalse() throws Exception {
    server.enqueue(new MockResponse());
    try (CurrentTraceContext.Scope ignored = httpTracing.tracing().currentTraceContext().newScope(
        TraceContext.newBuilder().traceId(1).spanId(1).sampled(false).build())) {
      get(client, "baz");
    }

    RecordedRequest request = server.takeRequest();
    assertThat(request.getHeaders().toMultimap())
        .containsKeys("x-b3-traceId", "x-b3-spanId")
        // .doesNotContainKey("x-b3-parentSpanId") AWS SDK tracing uses 2 spans, so there is a parent
        .containsEntry("x-b3-sampled", asList("0"));
  }

  @Override @Test public void addsErrorTagOnTransportException() throws Exception {
    super.addsErrorTagOnTransportException();
    Span applicationSpan = takeSpan();
    assertThat(applicationSpan.kind()).isNull();
    assertThat(applicationSpan.tags()).containsKey("error");
  }

  // This fails with NPE as we clear the client span on fail. on retry it isn't there anymore.
  // we should test that the client span IDs are different and match headers sent
  @Test @Ignore("TODO: we haven't properly addressed multiple client spans yet")
  public void retriesAreChildrenOfApplicationSpan() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse());

    get(client, "doo");

    Span client1 = takeSpan(), client2 = takeSpan(), applicationSpan = takeSpan();
    assertThat(applicationSpan.kind()).isNull();
    assertThat(applicationSpan.tags()).containsKey("error");
  }

  /** Body's inherently have a structure, and we use the operation name as the span name */
  @Override public void post() throws Exception {
    String path = "table";
    String body = "{\"TableName\":\"table\",\"Item\":{}}";
    server.enqueue(new MockResponse());

    post(client, path, body);

    assertThat(server.takeRequest().getBody().readUtf8())
        .isEqualTo(body);

    Span span = takeSpan();
    assertThat(span.remoteServiceName())
        .isEqualTo("dynamodb");
    assertThat(span.name())
        .isEqualTo("putitem");

    // application span
    span = takeSpan();
    assertThat(span.name()).isEqualTo("aws-sdk");
    assertThat(span.tags().get("aws.service_name"))
        .isEqualTo("DynamoDb");
    assertThat(span.tags().get("aws.operation"))
        .isEqualTo("PutItem");
  }

  @Override public void propagatesExtra_newTrace() throws Exception {
    super.propagatesExtra_newTrace();
    takeSpan();
  }

  @Override public void reportsClientKindToZipkin() throws Exception {
    super.reportsClientKindToZipkin();
    takeSpan();
  }

  @Override public void reportsSpanOnTransportException() throws Exception {
    super.reportsSpanOnTransportException();
    takeSpan();
  }

  /*
   * Tests that don't work
   */

  /** AWS doesn't use redirect */
  @Override public void redirect() {
  }

  /** Paths don't conform to expectation, always / */
  @Override public void customSampler() {
  }

  /** Unable to parse remote endpoint IP, would require DNS lookup */
  @Override public void reportsServerAddress() {
  }

  /** Path is always empty */
  @Override public void httpPathTagExcludesQueryParams() {
  }

  /** Error has exception instead of status code */
  @Override public void addsStatusCodeWhenNotOk() {
  }

  /** All http methods are POST */
  @Override public void defaultSpanNameIsMethodName() {
  }
}
