/**
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
package brave.instrumentation.aws;

import brave.http.HttpClientAdapter;
import com.amazonaws.Request;
import com.amazonaws.Response;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import zipkin2.Endpoint;

final class HttpAdapter extends HttpClientAdapter<Request<?>, Response<?>> {
  @Override public boolean parseServerAddress(Request<?> request, Endpoint.Builder builder) {
    return false;
  }

  @Override public String method(Request<?> request) {
    return request.getHttpMethod().name();
  }

  @Override public String path(Request<?> request) {
    return request.getResourcePath();
  }

  @Override public String url(Request<?> request) {
    StringBuilder url = new StringBuilder(request.getEndpoint().toString());
    if (request.getResourcePath() != null) url.append(request.getResourcePath());
    if (request.getParameters().isEmpty()) return url.toString();
    url.append('?');
    Iterator<Map.Entry<String, List<String>>> entries = request.getParameters().entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, List<String>> entry = entries.next();
      url.append(entry.getKey());
      if (entry.getKey().isEmpty()) continue;
      url.append('=').append(entry.getValue().get(0)); // skip the others.
      if (entries.hasNext()) url.append('&');
    }
    return url.toString();
  }

  @Override public String requestHeader(Request<?> request, String name) {
    return request.getHeaders().get(name);
  }

  @Override public Integer statusCode(Response<?> response) {
    return response.getHttpResponse().getStatusCode();
  }
}
