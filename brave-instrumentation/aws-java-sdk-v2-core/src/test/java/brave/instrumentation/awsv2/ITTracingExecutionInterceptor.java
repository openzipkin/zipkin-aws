/*
 * Copyright 2016-2020 The OpenZipkin Authors
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

import brave.SpanCustomizer;
import brave.http.HttpAdapter;
import brave.http.HttpClientParser;
import brave.http.HttpResponseParser;
import brave.http.HttpTags;
import brave.test.http.ITHttpClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import okhttp3.mockwebserver.MockResponse;
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
import zipkin2.Annotation;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class ITTracingExecutionInterceptor extends ITHttpClient<DynamoDbClient> {
  // TODO: make a TestClient which acts like the ones AWS generates, but uses features not in
  // Dynamo, such as HTTP paths and query parameters (that or switch to S3 or Route53, as they do).
  @Override protected DynamoDbClient newClient(int port) throws IOException {
    ClientOverrideConfiguration configuration = ClientOverrideConfiguration.builder()
        .retryPolicy(RetryPolicy.builder().numRetries(3).build())
        .apiCallTimeout(Duration.ofSeconds(1))
        .addExecutionInterceptor(AwsSdkTracing.create(httpTracing).executionInterceptor())
        .build();

    return client = DynamoDbClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .httpClient(primeHttpClient())
        .region(Region.US_EAST_1)
        .overrideConfiguration(configuration)
        .endpointOverride(URI.create("http://127.0.0.1:" + port))
        .build();
  }

  // For some reason, the first request will fail. This primes the connection until we know why
  SdkHttpClient primeHttpClient() throws IOException {
    server.enqueue(new MockResponse());
    SdkHttpClient httpClient = UrlConnectionHttpClient.builder().build();
    try {
      httpClient.prepareRequest(HttpExecuteRequest.builder()
          .request(SdkHttpRequest.builder()
              .method(SdkHttpMethod.GET)
              .uri(server.url("/").uri())
              .build())
          .build()).call();
    } finally {
      takeRequest();
    }
    return httpClient;
  }

  @Override protected void closeClient(DynamoDbClient client) {
    client.close();
  }

  @Override protected void get(DynamoDbClient client, String s) {
    client.getItem(GetItemRequest.builder().tableName(s).key(Collections.emptyMap()).build());
  }

  @Override protected void post(DynamoDbClient client, String s, String s1) {
    client.putItem(PutItemRequest.builder().tableName(s).item(Collections.emptyMap()).build());
  }

  /*
   * Tests are overridden due to reasons that should be revisited:
   *  * Modeling choices that overwrite user configuration, such as overwriting names
   *  * This test assumes AWS requests have "/" path with no parameters (not universally true ex S3)
   */

  @Test public void readsRequestAtResponseTime() throws IOException {
    String uri = "/"; // This test doesn't currently allow non-root HTTP paths

    closeClient(client);
    httpTracing = httpTracing.toBuilder()
        .clientResponseParser((response, context, span) -> {
          HttpTags.URL.tag(response.request(), span); // just the path is tagged by default
        })
        .build();

    client = newClient(server.getPort());
    server.enqueue(new MockResponse());
    get(client, uri);

    assertThat(reporter.takeRemoteSpan(Span.Kind.CLIENT).tags())
        .containsEntry("http.url", url(uri));
  }

  /** Service and span names don't conform to expectations. */
  @Override @Test public void supportsPortableCustomization() throws IOException {
    String uri = "/"; // This test doesn't currently allow non-root HTTP paths

    closeClient(client);
    httpTracing = httpTracing.toBuilder()
        .clientRequestParser((request, context, span) -> {
          span.name(request.method().toLowerCase() + " " + request.path());
          HttpTags.URL.tag(request, span); // just the path is tagged by default
          span.tag("request_customizer.is_span", (span instanceof brave.Span) + "");
        })
        .clientResponseParser((response, context, span) -> {
          HttpResponseParser.DEFAULT.parse(response, context, span);
          span.tag("response_customizer.is_span", (span instanceof brave.Span) + "");
        })
        .build().clientOf("remote-service");

    client = newClient(server.getPort());
    server.enqueue(new MockResponse());
    get(client, uri);

    Span span = reporter.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.name())
        .isEqualTo("getitem"); // Overwrites default span name

    assertThat(span.remoteServiceName())
        .isEqualTo("dynamodb"); // Ignores HttpTracing.serverName()

    assertThat(span.tags())
        .containsEntry("http.url", url(uri))
        .containsEntry("request_customizer.is_span", "false")
        .containsEntry("response_customizer.is_span", "false");
  }

  /** Service and span names don't conform to expectations. */
  @Override
  @Deprecated @Test public void supportsDeprecatedPortableCustomization() throws IOException {
    String uri = "/"; // This test doesn't currently allow non-root HTTP paths

    closeClient(client);
    httpTracing = httpTracing.toBuilder()
        .clientParser(new HttpClientParser() {
          @Override
          public <Req> void request(HttpAdapter<Req, ?> adapter, Req req,
              SpanCustomizer customizer) {
            customizer.name(adapter.method(req).toLowerCase() + " " + adapter.path(req));
            customizer.tag("http.url", adapter.url(req)); // just the path is tagged by default
            customizer.tag("context.visible", String.valueOf(currentTraceContext.get() != null));
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

    Span span = reporter.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.name())
        .isEqualTo("getitem"); // Overwrites default span name

    assertThat(span.remoteServiceName())
        .isEqualTo("dynamodb"); // Ignores HttpTracing.serverName()

    assertThat(span.tags())
        .containsEntry("http.url", url(uri))
        .containsEntry("context.visible", "true")
        .containsEntry("request_customizer.is_span", "false")
        .containsEntry("response_customizer.is_span", "false");
  }

  /** Body's inherently have a structure, and we use the operation name as the span name */
  @Override public void post() {
    String path = "table";
    String body = "{\"TableName\":\"table\",\"Item\":{}}";
    server.enqueue(new MockResponse());

    post(client, path, body);

    assertThat(takeRequest().getBody().readUtf8())
        .isEqualTo(body);

    Span span = reporter.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.remoteServiceName())
        .isEqualTo("dynamodb");
    assertThat(span.name())
        .isEqualTo("putitem");
    assertThat(span.tags().get("aws.service_name"))
        .isEqualTo("DynamoDb");
    assertThat(span.tags().get("aws.operation"))
        .isEqualTo("PutItem");
  }

  /** Make sure retry attempts are all annotated in the span. */
  @Test public void retriesAnnotated() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse());

    get(client, "/");

    Span span = reporter.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.remoteServiceName()).isEqualTo("dynamodb");
    assertThat(span.name()).isEqualTo("getitem");
    assertThat(span.annotations()).extracting(Annotation::value)
        .containsExactly("ws", "wr", "ws", "wr");

    // Ensure all requests have the injected headers.
    assertThat(server.takeRequest().getHeader("x-b3-spanid"))
        .isEqualTo(span.id());
    assertThat(server.takeRequest().getHeader("x-b3-spanid"))
        .isEqualTo(span.id());
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

  /** All http methods are POST */
  @Override public void defaultSpanNameIsMethodName() {
  }
}
