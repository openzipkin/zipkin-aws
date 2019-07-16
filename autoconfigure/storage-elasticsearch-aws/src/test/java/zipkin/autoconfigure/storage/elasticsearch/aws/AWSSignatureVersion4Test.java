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
package zipkin.autoconfigure.storage.elasticsearch.aws;

import com.linecorp.armeria.client.ClientRequestContext;
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
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit4.server.ServerRule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  @Rule public ExpectedException thrown = ExpectedException.none();

  String region = "us-east-1";
  AWSCredentials.Provider credentials = () -> new AWSCredentials("access-key", "secret-key", null);

  HttpClient client;

  @Before public void setUp() {
    client = new HttpClientBuilder(server.httpUri("/"))
        .decorator(AWSSignatureVersion4.newDecorator(region, "es", () -> credentials.get()))
        .build();
  }

  @Test
  public void propagatesExceptionGettingCredentials() {
    credentials =
        () -> {
          throw new IllegalStateException(
              "Unable to load AWS credentials from any provider in the chain");
        };

    assertThatThrownBy(() -> client.get("/").aggregate().join())
        .isInstanceOfSatisfying(CompletionException.class,
            t -> assertThat(t.getCause())
                // makes sure this isn't wrapped.
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unable to load AWS credentials from any provider in the chain"));
  }

  @Test
  public void unwrapsJsonError() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.FORBIDDEN,
        MediaType.JSON_UTF_8,
        "{\"message\":\"The request signature we calculated does not match the signature you "
            + "provided.\"}"));

    assertThatThrownBy(() -> client.get("/_template/zipkin_template").aggregate().join())
        .isInstanceOfSatisfying(CompletionException.class,
            t -> assertThat(t.getCause())
                // makes sure this isn't wrapped.
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The request signature we calculated does not match the signature you "
                    + "provided."));
  }

  @Test
  public void signsRequestsForRegionAndEsService() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(HttpStatus.OK));

    client.get("/_template/zipkin_template").aggregate().join();

    AggregatedHttpRequest request = CAPTURED_REQUEST.get();
    assertThat(request.headers().get(HttpHeaderNames.AUTHORIZATION))
        .startsWith("AWS4-HMAC-SHA256 Credential=" + credentials.get().accessKey)
        .contains(region + "/es/aws4_request"); // for the region and service
  }

  @Test
  public void canonicalString_commasInPath() {
    AggregatedHttpRequest request = AggregatedHttpRequest.of(
        RequestHeaders.builder(HttpMethod.POST,
            "/zipkin-2016-10-05,zipkin-2016-10-06/dependencylink/_search?allow_no_indices=true&expand_wildcards=open&ignore_unavailable=true")
            .set(HttpHeaderNames.HOST, "search-zipkin-2rlyh66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com")
            .set(AWSSignatureVersion4.X_AMZ_DATE, "20161004T132314Z")
            .contentType(MediaType.JSON_UTF_8)
        .build(),
        HttpData.ofUtf8("{\n" + "    \"query\" : {\n" + "      \"match_all\" : { }\n" + "    }")
    );
    ClientRequestContext ctx = ClientRequestContext.of(HttpRequest.of(request));
    ByteBuf result = Unpooled.buffer();

    AWSSignatureVersion4.writeCanonicalString(ctx, request.headers(), request.content(), result);
    // Ensure that the canonical string encodes commas with %2C
    assertThat(result.toString(StandardCharsets.UTF_8))
        .isEqualTo(
            "POST\n"
                + "/zipkin-2016-10-05%2Czipkin-2016-10-06/dependencylink/_search\n"
                + "allow_no_indices=true&expand_wildcards=open&ignore_unavailable=true\n"
                + "host:search-zipkin-2rlyh66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com\n"
                + "x-amz-date:20161004T132314Z\n"
                + "\n"
                + "host;x-amz-date\n"
                + "2fd35cb36e5de91bbae279313c371fb630a6b3aab1478df378c5e73e667a1747");
  }

  /** Starting with Zipkin 1.31 colons are used to delimit index types in ES */
  @Test
  public void canonicalString_colonsInPath() throws InterruptedException, IOException {
    AggregatedHttpRequest request = AggregatedHttpRequest.of(
        RequestHeaders.builder(HttpMethod.GET,
            "/_cluster/health/zipkin:span-*")
            .set(HttpHeaderNames.HOST, "search-zipkin53-mhdyquzbwwzwvln6phfzr3mmdi.ap-southeast-1.es.amazonaws.com")
            .set(AWSSignatureVersion4.X_AMZ_DATE, "20170830T143137Z")
            .build()
    );
    ClientRequestContext ctx = ClientRequestContext.of(HttpRequest.of(request));
    ByteBuf result = Unpooled.buffer();

    AWSSignatureVersion4.writeCanonicalString(ctx, request.headers(), request.content(), result);

    // Ensure that the canonical string encodes commas with %2C
    assertThat(result.toString(StandardCharsets.UTF_8))
        .isEqualTo(
            "GET\n"
                + "/_cluster/health/zipkin%3Aspan-%2A\n"
                + "\n"
                + "host:search-zipkin53-mhdyquzbwwzwvln6phfzr3mmdi.ap-southeast-1.es.amazonaws.com\n"
                + "x-amz-date:20170830T143137Z\n"
                + "\n"
                + "host;x-amz-date\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  }
}
