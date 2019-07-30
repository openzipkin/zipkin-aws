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

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.HttpStatusClass;
import com.linecorp.armeria.common.util.SafeCloseable;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.linecorp.armeria.client.Clients.withContextCustomizer;
import static com.linecorp.armeria.common.HttpMethod.GET;
import static zipkin.autoconfigure.storage.elasticsearch.aws.ZipkinElasticsearchAwsStorageAutoConfiguration.JSON;

final class ElasticsearchDomainEndpoint implements Supplier<EndpointGroup> {
  static final AttributeKey<String> NAME = AttributeKey.valueOf("name");

  final Function<Endpoint, HttpClient> clientFactory;
  final Endpoint endpoint;
  final String domain;

  ElasticsearchDomainEndpoint(Function<Endpoint, HttpClient> clientFactory, Endpoint endpoint,
      String domain) {
    this.clientFactory = clientFactory;
    this.endpoint = endpoint;
    this.domain = domain;
  }

  @Override public DnsAddressEndpointGroup get() {
    // We want "string = GET /2015-01-01/es/domain/{domain}" labeled as "es-get-domain"

    // The domain endpoint is read only once per startup. Hence, there is less impact to allocating
    // strings. We retain the string so that it can be logged if the AWS response is malformed.
    HttpStatus status;
    String body;

    AggregatedHttpRequest req = AggregatedHttpRequest.of(GET, "/2015-01-01/es/domain/" + domain);
    try (SafeCloseable sc = withContextCustomizer(ctx -> ctx.attr(NAME).set("es-get-domain"))) {
      AggregatedHttpResponse res = clientFactory.apply(endpoint).execute(req).aggregate().join();
      status = res.status();
      body = res.contentUtf8();
    } catch (RuntimeException | Error e) {
      throw new RuntimeException("couldn't lookup AWS ES domain endpoint",
          e instanceof CompletionException ? e.getCause() : e
      );
    }

    if (!status.codeClass().equals(HttpStatusClass.SUCCESS)) {
      String message = req.path() + " failed with status " + status;
      if (!body.isEmpty()) message += ": " + body;
      throw new RuntimeException(message);
    }

    String endpoint;
    try {
      JsonNode root = JSON.readTree(body);
      endpoint = root.at("/DomainStatus/Endpoints/vpc").textValue();
      if (endpoint == null) endpoint = root.at("/DomainStatus/Endpoint").textValue();
    } catch (IOException e) {
      throw new AssertionError("Unexpected to have IOException reading a string", e);
    }

    if (endpoint == null) {
      throw new RuntimeException(
          "Neither DomainStatus.Endpoints.vpc nor DomainStatus.Endpoint were present in response: "
              + body);
    }

    return DnsAddressEndpointGroup.of(endpoint);
  }
}
