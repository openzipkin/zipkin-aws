/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.awsv2;

import brave.Span;
import brave.handler.MutableSpan;
import brave.http.HttpResponseParser;
import brave.http.HttpTags;
import brave.test.http.ITHttpClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.Test;
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

  @Override protected void options(DynamoDbClient client, String s) {
    throw new AssumptionViolatedException(
        "HTTP OPTIONS method isn't implemented at this abstraction");
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

    assertThat(testSpanHandler.takeRemoteSpan(Span.Kind.CLIENT).tags())
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

    MutableSpan span = testSpanHandler.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.name())
        .isEqualTo("GetItem"); // Overwrites default span name

    assertThat(span.remoteServiceName())
        .isEqualTo("DynamoDb"); // Ignores HttpTracing.serverName()

    assertThat(span.tags())
        .containsEntry("http.url", url(uri))
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

    MutableSpan span = testSpanHandler.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.remoteServiceName())
        .isEqualTo("DynamoDb");
    assertThat(span.name())
        .isEqualTo("PutItem");
    assertThat(span.tags().get("aws.service_name"))
        .isEqualTo("DynamoDb");
    assertThat(span.tags().get("aws.operation"))
        .isEqualTo("PutItem");
  }

  /** Make sure retry attempts are all annotated in the span. */
  @Test void retriesAnnotated() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(500));
    server.enqueue(new MockResponse());

    get(client, "/");

    MutableSpan span = testSpanHandler.takeRemoteSpan(Span.Kind.CLIENT);
    assertThat(span.remoteServiceName()).isEqualTo("DynamoDb");
    assertThat(span.name()).isEqualTo("GetItem");
    assertThat(span.annotations()).extracting(Map.Entry::getValue)
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
