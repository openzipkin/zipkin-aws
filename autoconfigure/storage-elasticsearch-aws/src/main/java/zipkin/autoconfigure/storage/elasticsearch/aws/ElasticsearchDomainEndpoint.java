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
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.HttpStatusClass;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.logging.Logger;
import zipkin2.elasticsearch.ElasticsearchStorage.HostsSupplier;

import static zipkin.autoconfigure.storage.elasticsearch.aws.ZipkinElasticsearchAwsStorageAutoConfiguration.JSON;

final class ElasticsearchDomainEndpoint implements HostsSupplier {
  static final Logger log = Logger.getLogger(ElasticsearchDomainEndpoint.class.getName());

  final HttpClient client;
  final AggregatedHttpRequest describeElasticsearchDomain;

  ElasticsearchDomainEndpoint(HttpClient client, String domain) {
    if (client == null) throw new NullPointerException("client == null");
    if (domain == null) throw new NullPointerException("domain == null");
    this.client = client;
    this.describeElasticsearchDomain =
        AggregatedHttpRequest.of(HttpMethod.GET, "/2015-01-01/es/domain/" + domain);
  }

  @Override public List<String> get() {
    HttpStatus status;
    String body;
    try {
      AggregatedHttpResponse res = client.execute(describeElasticsearchDomain).aggregate().join();
      status = res.status();
      // As the domain endpoint is read only once per startup. We don't worry about pooling etc.
      // This allows for easier debugging and less try/finally state management.
      body = res.contentUtf8();
    } catch (RuntimeException | Error e) {
      throw new RuntimeException("couldn't lookup AWS ES domain endpoint",
          e instanceof CompletionException ? e.getCause() : e
      );
    }

    if (!status.codeClass().equals(HttpStatusClass.SUCCESS)) {
      String message = describeElasticsearchDomain.path() + " failed with status " + status;
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

    // While not strictly defined, in practice, AWS ES endpoints are host names, not urls. This is
    // likely because they listen on both 80 and 443. Hence the below is overly defensive except.
    // https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-configuration-api.html#es-configuration-api-datatypes-endpointsmap
    if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
      endpoint = "https://" + endpoint;
    }

    log.fine("using endpoint " + endpoint);
    return Collections.singletonList(endpoint);
  }
}
