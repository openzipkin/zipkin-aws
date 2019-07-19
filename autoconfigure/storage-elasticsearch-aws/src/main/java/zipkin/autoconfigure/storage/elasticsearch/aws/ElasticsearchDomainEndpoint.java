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
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatusClass;
import com.squareup.moshi.JsonReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.logging.Logger;
import okio.Buffer;
import zipkin2.elasticsearch.ElasticsearchStorage.HostsSupplier;

import static zipkin2.elasticsearch.internal.JsonReaders.enterPath;

final class ElasticsearchDomainEndpoint implements HostsSupplier {
  static final Logger log = Logger.getLogger(ElasticsearchDomainEndpoint.class.getName());

  final HttpClient client;
  final AggregatedHttpRequest describeElasticsearchDomain;
  volatile String endpoint;

  ElasticsearchDomainEndpoint(HttpClient client, String domain) {
    if (client == null) throw new NullPointerException("client == null");
    if (domain == null) throw new NullPointerException("domain == null");
    this.client = client;
    this.describeElasticsearchDomain =
        AggregatedHttpRequest.of(HttpMethod.GET, "/2015-01-01/es/domain/" + domain);
  }

  @Override public List<String> get() { // memoized on success to prevent redundant calls
    if (endpoint == null) {
      synchronized (this) {
        if (endpoint == null) {
          endpoint = getEndpoint();
        }
      }
    }
    return Collections.singletonList(endpoint);
  }

  String getEndpoint() {
    final AggregatedHttpResponse response;
    try {
      response = client.execute(describeElasticsearchDomain)
          .aggregate().join();

      String body = response.contentUtf8();
      if (!response.status().codeClass().equals(HttpStatusClass.SUCCESS)) {
        String message = describeElasticsearchDomain.path()
            + " failed with status "
            + response.status();
        if (!body.isEmpty()) message += ": " + body;
        throw new IllegalStateException(message);
      }
      JsonReader endpointReader = JsonReader.of(new Buffer().writeUtf8(body));
      endpointReader = enterPath(endpointReader, "DomainStatus", "Endpoints");
      if (endpointReader != null) endpointReader = enterPath(endpointReader, "vpc");

      if (endpointReader == null) {
        endpointReader =
            enterPath(JsonReader.of(new Buffer().writeUtf8(body)), "DomainStatus", "Endpoint");
      }

      if (endpointReader == null) {
        throw new RuntimeException(
            "Neither DomainStatus.Endpoints.vpc nor DomainStatus.Endpoint were present in response: "
                + body);
      }

      String endpoint = endpointReader.nextString();
      if (!endpoint.startsWith("https://")) {
        endpoint = "https://" + endpoint;
      }
      log.fine("using endpoint " + endpoint);
      return endpoint;
    } catch (CompletionException e) {
      throw new RuntimeException("couldn't lookup AWS ES domain endpoint", e.getCause());
    } catch (IOException e) {
      throw new UncheckedIOException("couldn't lookup AWS ES domain endpoint", e);
    }
  }
}
