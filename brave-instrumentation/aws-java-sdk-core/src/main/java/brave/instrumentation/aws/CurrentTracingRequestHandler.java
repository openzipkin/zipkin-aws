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

import brave.Tracing;
import brave.http.HttpClientHandler;
import brave.http.HttpTracing;
import brave.propagation.TraceContext;
import com.amazonaws.Request;
import com.amazonaws.Response;

public class CurrentTracingRequestHandler extends TracingRequestHandler {

  private HttpTracing httpTracing;
  private HttpClientHandler<Request<?>, Response<?>> handler;
  private TraceContext.Injector<Request<?>> injector;

  public CurrentTracingRequestHandler() {
  }

  private HttpTracing httpTracing() {
    if (httpTracing == null) {
      if (Tracing.current() != null) {
        httpTracing = HttpTracing.create(Tracing.current());
      }
    }
    return httpTracing;
  }

  @Override protected HttpClientHandler<Request<?>, Response<?>> handler() {
    if (handler == null) {
      HttpTracing httpTracing = httpTracing();
      if (httpTracing != null) {
        handler = HttpClientHandler.create(httpTracing, ADAPTER);
      }
    }
    return handler;
  }

  @Override protected TraceContext.Injector<Request<?>> injector() {
    if (injector == null) {
      HttpTracing httpTracing = httpTracing();
      if (httpTracing != null) {
        injector = httpTracing.tracing().propagation().injector(SETTER);
      }
    }
    return injector;
  }
}
