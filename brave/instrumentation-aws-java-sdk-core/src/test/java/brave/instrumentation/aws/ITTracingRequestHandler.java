/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.aws;

import brave.Span;
import brave.handler.MutableSpan;
import brave.http.HttpResponseParser;
import brave.http.HttpTags;
import brave.test.http.ITHttpClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import java.io.IOException;
import java.util.Collections;
import okhttp3.mockwebserver.MockResponse;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ITTracingRequestHandler extends ITHttpClient<AmazonDynamoDB> {
  @Override protected AmazonDynamoDB newClient(int i) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setMaxErrorRetry(0);
    clientConfiguration.setRequestTimeout(1000);

    return AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "y")))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:" + i, "us-east-1"))
        .withRequestHandlers(new TracingRequestHandler(httpTracing))
        .withClientConfiguration(clientConfiguration)
        .build();
  }

  @Override protected void closeClient(AmazonDynamoDB dynamoDB) {
    dynamoDB.shutdown();
  }

  @Override protected void get(AmazonDynamoDB dynamoDB, String s) {
    dynamoDB.getItem(s, Collections.emptyMap());
  }

  @Override protected void options(AmazonDynamoDB dynamoDB, String s) {
    throw new AssumptionViolatedException(
        "HTTP OPTIONS method isn't implemented at this abstraction");
  }

  @Override protected void post(AmazonDynamoDB dynamoDB, String s, String s1) {
    dynamoDB.putItem(s, Collections.emptyMap());
  }

  /*
   * Tests are overridden due to reasons that should be revisited:
   *  * Modeling choices that overwrite user configuration, such as overwriting names
   *  * This test assumes AWS requests have "/" path with no parameters (not universally true ex S3)
   */

  @Override public void emptyPath() throws IOException {
    super.emptyPath();
    testSpanHandler.takeLocalSpan();
  }

  /** Service and span names don't conform to expectations. */
  @Override @Test public void supportsPortableCustomization() {
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
        .isEqualTo("AmazonDynamoDBv2"); // Ignores HttpTracing.serverName()

    assertThat(span.tags())
        .containsEntry("http.url", url(uri))
        .containsEntry("request_customizer.is_span", "false")
        .containsEntry("response_customizer.is_span", "false");

    testSpanHandler.takeLocalSpan();
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
        .isEqualTo("AmazonDynamoDBv2");
    assertThat(span.name())
        .isEqualTo("PutItem");

    assertThat(testSpanHandler.takeLocalSpan().tags())
        .containsEntry("aws.service_name", "AmazonDynamoDBv2")
        .containsEntry("aws.operation", "PutItem");
  }

  @Override public void propagatesBaggage() throws IOException {
    super.propagatesBaggage();
    testSpanHandler.takeLocalSpan();
  }

  @Override public void reportsClientKindToZipkin() throws IOException {
    super.reportsClientKindToZipkin();
    testSpanHandler.takeLocalSpan();
  }

  @Override public void spanHandlerSeesError() {
    assertThatThrownBy(super::spanHandlerSeesError)
        .isInstanceOf(AssertionError.class)
        // spanHandler was called twice. OK because the second time was for the application span.
        .hasMessageContaining("Expected size: 1 but was: 2");

    assertThat(testSpanHandler.takeLocalSpan().error())
        .hasMessageStartingWith("Unable to execute HTTP request");
  }

  @Override public void setsError_onTransportException() {
    super.setsError_onTransportException();
    assertThat(testSpanHandler.takeLocalSpan().error())
        .hasMessageStartingWith("Unable to execute HTTP request");
  }

  /*
   * Tests that don't work because there's an application span above the HTTP requests
   */
  @Override public void propagatesUnsampledContext() {
  }

  @Override public void clientTimestampAndDurationEnclosedByParent() {
  }

  @Override public void propagatesNewTrace() {
  }

  @Override public void propagatesChildOfCurrentSpan() {
  }

  /*
   * Tests that don't work for other reasons
   */

  /** unwrapping Amazon's default exception for HTTP status isn't implemented */
  @Override public void addsStatusCodeWhenNotOk() {
  }

  /** usual problem where the url is wrong as it doesn't include the query */
  @Override public void readsRequestAtResponseTime() {
  }

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
