/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
package brave.instrumentation.awsv2;

import brave.http.HttpTracing;
import brave.internal.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

public final class AwsSdkTracing {
  public static AwsSdkTracing create(HttpTracing httpTracing) {
    return new AwsSdkTracing(httpTracing);
  }

  final HttpTracing httpTracing;

  AwsSdkTracing(HttpTracing httpTracing) {
    if (httpTracing == null) throw new NullPointerException("httpTracing == null");
    this.httpTracing = httpTracing;
  }

  public ExecutionInterceptor executionInterceptor() {
    return new TracingExecutionInterceptor(httpTracing);
  }

  static final class HttpClientRequest extends brave.http.HttpClientRequest {
    final SdkHttpRequest delegate;
    SdkHttpRequest.Builder builder;

    HttpClientRequest(SdkHttpRequest delegate) {
      this.delegate = delegate;
    }

    @Override public Object unwrap() {
      return delegate;
    }

    @Override public String method() {
      return delegate.method().name();
    }

    @Override public String path() {
      return delegate.encodedPath();
    }

    @Override public String url() {
      StringBuilder url = new StringBuilder(delegate.protocol())
          .append("://")
          .append(delegate.host())
          .append(":")
          .append(delegate.port());
      if (delegate.encodedPath() != null) url.append(delegate.encodedPath());
      if (delegate.rawQueryParameters().isEmpty()) return url.toString();
      url.append('?');
      Iterator<Map.Entry<String, List<String>>> entries =
          delegate.rawQueryParameters().entrySet().iterator();
      while (entries.hasNext()) {
        Map.Entry<String, List<String>> entry = entries.next();
        url.append(entry.getKey());
        if (entry.getKey().isEmpty()) continue;
        url.append('=').append(entry.getValue().get(0)); // skip the others.
        if (entries.hasNext()) url.append('&');
      }
      return url.toString();
    }

    @Override public String header(String name) {
      List<String> values = delegate.headers().get(name);
      return values != null && !values.isEmpty() ? values.get(0) : null;
    }

    @Override public void header(String name, String value) {
      if (builder == null) builder = delegate.toBuilder();
      builder.putHeader(name, value);
    }

    SdkHttpRequest build() {
      return builder != null ? builder.build() : delegate;
    }
  }

  static final class HttpClientResponse extends brave.http.HttpClientResponse {
    @Nullable final SdkHttpRequest request;
    @Nullable final SdkHttpResponse response;
    @Nullable final Throwable error;

    HttpClientResponse(
        @Nullable SdkHttpRequest request,
        @Nullable SdkHttpResponse response,
        @Nullable Throwable error
    ) {
      if (response == null && error == null) {
        throw new NullPointerException("response == null && error == null");
      }
      this.request = request;
      this.response = response;
      this.error = error;
    }

    @Override public HttpClientRequest request() {
      return request != null ? new HttpClientRequest(request) : null;
    }

    @Override public Throwable error() {
      return error;
    }

    @Override public Object unwrap() {
      return response;
    }

    @Override public int statusCode() {
      return response.statusCode();
    }
  }
}
