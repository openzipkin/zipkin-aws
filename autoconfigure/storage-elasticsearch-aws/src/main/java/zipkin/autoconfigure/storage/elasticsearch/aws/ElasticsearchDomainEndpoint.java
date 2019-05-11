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

import com.squareup.moshi.JsonReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.Buffer;
import zipkin2.elasticsearch.ElasticsearchStorage;

import static zipkin2.elasticsearch.internal.JsonReaders.enterPath;

final class ElasticsearchDomainEndpoint implements ElasticsearchStorage.HostsSupplier {
  static final Logger log = Logger.getLogger(ElasticsearchDomainEndpoint.class.getName());

  final OkHttpClient client;
  final Request describeElasticsearchDomain;

  ElasticsearchDomainEndpoint(OkHttpClient client, HttpUrl baseUrl, String domain) {
    if (client == null) throw new NullPointerException("client == null");
    if (baseUrl == null) throw new NullPointerException("baseUrl == null");
    if (domain == null) throw new NullPointerException("domain == null");
    this.client = client;
    this.describeElasticsearchDomain =
        new Request.Builder()
            .url(baseUrl.newBuilder("2015-01-01/es/domain").addPathSegment(domain).build())
            .tag("get-es-domain")
            .build();
  }

  @Override
  public List<String> get() {
    try (Response response = client.newCall(describeElasticsearchDomain).execute()) {
      String body = response.body().string();
      if (!response.isSuccessful()) {
        String message =
            describeElasticsearchDomain.url().encodedPath()
                + " failed with status "
                + response.code();
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
        throw new IllegalStateException(
            "Neither DomainStatus.Endpoints.vpc nor DomainStatus.Endpoint were present in response: "
                + body);
      }

      String endpoint = endpointReader.nextString();
      if (!endpoint.startsWith("https://")) {
        endpoint = "https://" + endpoint;
      }
      log.fine("using endpoint " + endpoint);
      return Collections.singletonList(endpoint);
    } catch (IOException e) {
      throw new IllegalStateException("couldn't lookup domain endpoint", e);
    }
  }
}
