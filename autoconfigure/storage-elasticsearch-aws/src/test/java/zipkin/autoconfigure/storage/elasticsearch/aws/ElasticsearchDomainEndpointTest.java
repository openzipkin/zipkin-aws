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

import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit4.server.ServerRule;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchDomainEndpointTest {

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

  ElasticsearchDomainEndpoint client;

  @Before public void setUp() {
    client = new ElasticsearchDomainEndpoint(HttpClient.of(server.httpUri("/")), "zipkin53");
  }

  @Test public void publicUrl() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        "{\n"
            + "  \"DomainStatus\": {\n"
            + "    \"Endpoint\": \"search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com\",\n"
            + "    \"Endpoints\": null\n"
            + "  }\n"
            + "}"));

    assertThat(client.get())
        .containsExactly(
            "https://search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com");
  }

  // Amazon ES endpoints don't actually return http or https prefix.
  @Test public void fakeUrl() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        "{\n"
            + "  \"DomainStatus\": {\n"
            + "    \"Endpoint\": \"https://search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com\",\n"
            + "    \"Endpoints\": null\n"
            + "  }\n"
            + "}"));

    assertThat(client.get())
        .containsExactly(
            "https://search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com");
  }

  // Amazon ES endpoints don't actually return http or https prefix.
  @Test public void fakeUrl_http() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        "{\n"
            + "  \"DomainStatus\": {\n"
            + "    \"Endpoint\": \"http://search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com\",\n"
            + "    \"Endpoints\": null\n"
            + "  }\n"
            + "}"));

    assertThat(client.get())
        .containsExactly(
            "http://search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com");
  }

  @Test public void vpcUrl() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        "{\n"
            + "  \"DomainStatus\": {\n"
            + "    \"Endpoint\": null,\n"
            + "    \"Endpoints\": {\n"
            + "      \"vpc\":\"search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com\"\n"
            + "    }\n"
            + "  }\n"
            + "}"));

    assertThat(client.get())
        .containsExactly(
            "https://search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com");
  }

  @Test public void vpcPreferred() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        "{\n"
            + "  \"DomainStatus\": {\n"
            + "    \"Endpoint\": \"isnotvpc\",\n"
            + "    \"Endpoints\": {\n"
            + "      \"vpc\":\"isvpc\"\n"
            + "    }\n"
            + "  }\n"
            + "}"));

    assertThat(client.get())
        .containsExactly("https://isvpc");
  }

  @Test public void vpcMissing() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        "{\n"
            + "  \"DomainStatus\": {\n"
            + "    \"Endpoint\": \"isnotvpc\",\n"
            + "    \"Endpoints\": {}\n"
            + "  }\n"
            + "}"));

    assertThat(client.get())
        .containsExactly("https://isnotvpc");
  }

  /** Not quite sure why, but some have reported receiving no URLs at all */
  @Test public void noUrl() {
    // simplified.. URL is usually the only thing actually missing
    String body = "{\"DomainStatus\": {}}";
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        body));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        "Neither DomainStatus.Endpoints.vpc nor DomainStatus.Endpoint were present in response: "
            + body);

    client.get();
  }

  /** Not quite sure why, but some have reported receiving no URLs at all */
  @Test public void unauthorizedNoMessage() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(HttpStatus.FORBIDDEN));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("/2015-01-01/es/domain/zipkin53 failed with status 403");

    client.get();
  }
}
