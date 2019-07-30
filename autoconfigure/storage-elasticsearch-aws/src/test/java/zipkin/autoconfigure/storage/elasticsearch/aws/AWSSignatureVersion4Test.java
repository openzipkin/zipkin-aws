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
package zipkin.autoconfigure.storage.elasticsearch.aws;

import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.ClientRequestContextBuilder;
import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.client.HttpClientBuilder;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit4.server.ServerRule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin.autoconfigure.storage.elasticsearch.aws.AWSSignatureVersion4.writeCanonicalString;

public class AWSSignatureVersion4Test {

  static final AtomicReference<AggregatedHttpRequest> CAPTURED_REQUEST =
      new AtomicReference<>();
  static final AtomicReference<AggregatedHttpResponse> MOCK_RESPONSE =
      new AtomicReference<>();

  @ClassRule public static ServerRule server = new ServerRule() {
    @Override protected void configure(ServerBuilder sb) {
      sb.serviceUnder("/", (ctx, req) -> HttpResponse.from(
          req.aggregate().thenApply(agg -> {
            CAPTURED_REQUEST.set(agg);
            return HttpResponse.of(MOCK_RESPONSE.get());
          })));
    }
  };

  String region = "us-east-1";
  AWSCredentials.Provider credentials = () -> new AWSCredentials("access-key", "secret-key", null);

  HttpClient client;

  @Before public void setUp() {
    // Make a manual endpoint so that we can get a hostname
    Endpoint endpoint = Endpoint.of(
        "search-zipkin-2rlyh66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com",
        server.httpPort())
        .withIpAddr("127.0.0.1");

    client = new HttpClientBuilder(SessionProtocol.HTTP, endpoint)
        .decorator(AWSSignatureVersion4.newDecorator(region, () -> credentials.get()))
        .build();
  }

  @Test public void propagatesExceptionGettingCredentials() {
    credentials = () -> {
      throw new RuntimeException(
          "Unable to load AWS credentials from any provider in the chain");
    };

    assertThatThrownBy(() -> client.get("/").aggregate().join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Unable to load AWS credentials from any provider in the chain");
  }

  @Test public void unwrapsJsonError() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.FORBIDDEN,
        MediaType.JSON_UTF_8,
        "{\"message\":\"The request signature we calculated does not match the signature you "
            + "provided.\"}"));

    assertThatThrownBy(() -> client.get("/_template/zipkin_template").aggregate().join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "/_template/zipkin_template failed: The request signature we calculated does not match the signature you provided.");
  }

  @Test public void safeDriftedBody() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.FORBIDDEN,
        MediaType.JSON_UTF_8, "{\"msg\":\"I'm a clone\"}"));

    assertThatThrownBy(() -> client.get("/_template/zipkin_template").aggregate().join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("/_template/zipkin_template failed: 403 Forbidden");
  }

  @Test public void safeMissingResponseBody() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(HttpStatus.FORBIDDEN));

    assertThatThrownBy(() -> client.get("/_template/zipkin_template").aggregate().join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("/_template/zipkin_template failed: 403 Forbidden");
  }

  @Test public void signsRequestsForRegionAndEsService() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(HttpStatus.OK));

    client.get("/_template/zipkin_template").aggregate().join();

    AggregatedHttpRequest request = CAPTURED_REQUEST.get();
    assertThat(request.headers().get(HttpHeaderNames.AUTHORIZATION))
        .startsWith("AWS4-HMAC-SHA256 Credential=" + credentials.get().accessKey)
        .contains(region + "/es/aws4_request"); // for the region and service
  }

  @Test public void addsAwsDateHeader() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(HttpStatus.OK));

    client.get("/_template/zipkin_template").aggregate().join();

    assertThat(CAPTURED_REQUEST.get().headers().get("x-amz-date")).isNotNull();
  }

  @Test public void canonicalString_commasInPath() {
    AggregatedHttpRequest request = AggregatedHttpRequest.of(
        RequestHeaders.builder(HttpMethod.POST,
            "/zipkin-2016-10-05,zipkin-2016-10-06/dependencylink/_search?allow_no_indices=true&expand_wildcards=open&ignore_unavailable=true")
            .set(AWSSignatureVersion4.X_AMZ_DATE, "20161004T132314Z")
            .contentType(MediaType.JSON_UTF_8)
            .build(),
        HttpData.ofUtf8("{\n" + "    \"query\" : {\n" + "      \"match_all\" : { }\n" + "    }")
    );
    ClientRequestContext ctx = ClientRequestContextBuilder.of(HttpRequest.of(request))
        .endpoint(Endpoint.of(
            "search-zipkin-2rlyh66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com"))
        .build();

    ByteBuf result = Unpooled.buffer();

    writeCanonicalString(ctx, request.headers(), request.content(), result);
    // Ensure that the canonical string encodes commas with %2C
    assertThat(result.toString(UTF_8)).isEqualTo(""
        + "POST\n"
        + "/zipkin-2016-10-05%2Czipkin-2016-10-06/dependencylink/_search\n"
        + "allow_no_indices=true&expand_wildcards=open&ignore_unavailable=true\n"
        + "host:search-zipkin-2rlyh66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com\n"
        + "x-amz-date:20161004T132314Z\n"
        + "\n"
        + "host;x-amz-date\n"
        + "2fd35cb36e5de91bbae279313c371fb630a6b3aab1478df378c5e73e667a1747");
  }

  /** Starting with Zipkin 1.31 colons are used to delimit index types in ES */
  @Test public void canonicalString_colonsInPath() {
    AggregatedHttpRequest request = AggregatedHttpRequest.of(RequestHeaders.builder(HttpMethod.GET,
        "/_cluster/health/zipkin:span-*")
        .set(AWSSignatureVersion4.X_AMZ_DATE, "20170830T143137Z")
        .build()
    );
    ClientRequestContext ctx = ClientRequestContextBuilder.of(HttpRequest.of(request))
        .endpoint(Endpoint.of(
            "search-zipkin53-mhdyquzbwwzwvln6phfzr3mmdi.ap-southeast-1.es.amazonaws.com"))
        .build();

    ByteBuf result = Unpooled.buffer();

    writeCanonicalString(ctx, request.headers(), request.content(), result);

    // Ensure that the canonical string encodes commas with %2C
    assertThat(result.toString(UTF_8)).isEqualTo(""
        + "GET\n"
        + "/_cluster/health/zipkin%3Aspan-%2A\n"
        + "\n"
        + "host:search-zipkin53-mhdyquzbwwzwvln6phfzr3mmdi.ap-southeast-1.es.amazonaws.com\n"
        + "x-amz-date:20170830T143137Z\n"
        + "\n"
        + "host;x-amz-date\n"
        + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  }

  @Test public void canonicalString_getDomain() throws Exception {
    String timestamp = "20190730T134617Z";
    String yyyyMMdd = timestamp.substring(0, 8);
    AggregatedHttpRequest request = AggregatedHttpRequest.of(
        RequestHeaders.builder(HttpMethod.GET, "/2015-01-01/es/domain/zipkin")
            .set(AWSSignatureVersion4.X_AMZ_DATE, timestamp)
            .build()
    );
    ClientRequestContext ctx = ClientRequestContextBuilder.of(HttpRequest.of(request))
        .endpoint(Endpoint.of("es.ap-southeast-1.amazonaws.com"))
        .build();

    ByteBuf canonicalString = Unpooled.buffer();

    writeCanonicalString(ctx, request.headers(), request.content(), canonicalString);
    assertThat(canonicalString.toString(UTF_8)).isEqualTo(""
        + "GET\n"
        + "/2015-01-01/es/domain/zipkin\n"
        + "\n"
        + "host:es.ap-southeast-1.amazonaws.com\n"
        + "x-amz-date:" + timestamp + "\n"
        + "\n"
        + "host;x-amz-date\n"
        + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    ByteBuf toSign = Unpooled.buffer();
    AWSSignatureVersion4.writeToSign(timestamp,
        AWSSignatureVersion4.credentialScope(yyyyMMdd, "ap-southeast-1"), canonicalString, toSign);

    assertThat(toSign.toString(UTF_8)).isEqualTo(""
        + "AWS4-HMAC-SHA256\n"
        + "20190730T134617Z\n"
        + "20190730/ap-southeast-1/es/aws4_request\n"
        + "129dd8ded740553cd28544b4000982b8f88d7199b36a013fa89ee8e56c23f80e");
  }
}
