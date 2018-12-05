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
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

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
}
