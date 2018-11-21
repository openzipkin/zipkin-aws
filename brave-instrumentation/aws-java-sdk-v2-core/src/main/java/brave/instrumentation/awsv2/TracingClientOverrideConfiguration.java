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
package brave.instrumentation.awsv2;

import brave.http.HttpTracing;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

public class TracingClientOverrideConfiguration {
  public static TracingClientOverrideConfiguration create(HttpTracing httpTracing) {
    return new TracingClientOverrideConfiguration(httpTracing);
  }

  final HttpTracing httpTracing;

  TracingClientOverrideConfiguration(HttpTracing httpTracing) {
    if (httpTracing == null) throw new NullPointerException("httpTracing == null");
    this.httpTracing = httpTracing;
  }

  public ClientOverrideConfiguration build(ClientOverrideConfiguration.Builder builder) {
    if (builder == null) throw new NullPointerException("builder == null");
    builder.addExecutionInterceptor(new TracingExecutionInterceptor(httpTracing));

    return builder.build();
  }
}
