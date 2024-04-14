/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.elasticsearch;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.linecorp.armeria.common.SessionProtocol.HTTP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ElasticsearchDomainEndpointTest {

  static final AtomicReference<AggregatedHttpRequest> CAPTURED_REQUEST =
      new AtomicReference<>();
  static final AtomicReference<AggregatedHttpResponse> MOCK_RESPONSE =
      new AtomicReference<>();

  @RegisterExtension static ServerExtension server = new ServerExtension() {
    @Override protected void configure(ServerBuilder sb) {
      sb.serviceUnder("/", (ctx, req) -> HttpResponse.of(
          req.aggregate().thenApply(agg -> {
            CAPTURED_REQUEST.set(agg);
            return MOCK_RESPONSE.get().toHttpResponse();
          })));
    }
  };

  ElasticsearchDomainEndpoint client;

  @BeforeEach public void setUp() {
    client = new ElasticsearchDomainEndpoint((endpoint) -> WebClient.of(HTTP, endpoint),
        Endpoint.of("localhost", server.httpPort()), "ap-southeast-1", "zipkin53");
  }

  @Test void niceToString() {
    assertThat(client).hasToString("aws://ap-southeast-1/zipkin53");
  }

  @Test void publicUrl() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        """
        {
          "DomainStatus": {
            "Endpoint": "search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com",
            "Endpoints": null
          }
        }\
        """));

    assertThat(client.get()).extracting("hostname")
        .isEqualTo(
            "search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com");
  }

  @Test void vpcUrl() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        """
        {
          "DomainStatus": {
            "Endpoint": null,
            "Endpoints": {
              "vpc":"search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com"
            }
          }
        }\
        """));

    assertThat(client.get()).extracting("hostname")
        .isEqualTo(
            "search-zipkin53-mhdyquzbwwzwvln6phfzr3lldi.ap-southeast-1.es.amazonaws.com");
  }

  @Test void vpcPreferred() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        """
        {
          "DomainStatus": {
            "Endpoint": "isnotvpc",
            "Endpoints": {
              "vpc":"isvpc"
            }
          }
        }\
        """));

    assertThat(client.get()).extracting("hostname")
        .isEqualTo("isvpc");
  }

  @Test void vpcMissing() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        """
        {
          "DomainStatus": {
            "Endpoint": "isnotvpc",
            "Endpoints": {}
          }
        }\
        """));

    assertThat(client.get()).extracting("hostname")
        .isEqualTo("isnotvpc");
  }

  /** Not quite sure why, but some have reported receiving no URLs at all */
  @Test void noUrl() {
    // simplified.. URL is usually the only thing actually missing
    String body = "{\"DomainStatus\": {}}";
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        body));

    assertThatThrownBy(client::get)
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Neither DomainStatus.Endpoints.vpc nor DomainStatus.Endpoint were present in response: "
                + body);
  }

  /** Not quite sure why, but some have reported receiving no URLs at all */
  @Test void unauthorizedNoMessage() {
    MOCK_RESPONSE.set(AggregatedHttpResponse.of(HttpStatus.FORBIDDEN));

    assertThatThrownBy(client::get)
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith("/2015-01-01/es/domain/zipkin53 failed with status 403");
  }
}
