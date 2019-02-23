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
package brave.instrumentation.aws;

import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.client.builder.AwsAsyncClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.ExecutorFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class AwsClientTracing {
  public static AwsClientTracing create(HttpTracing httpTracing) {
    return new AwsClientTracing(httpTracing); // no builder yet as we don't need it yet.
  }

  static final ClientConfigurationFactory defaultClientConfigurationFactory =
      new ClientConfigurationFactory();

  final HttpTracing httpTracing;
  final CurrentTraceContext currentTraceContext;

  AwsClientTracing(HttpTracing httpTracing) { // intentionally hidden constructor
    if (httpTracing == null) throw new NullPointerException("httpTracing == null");
    this.httpTracing = httpTracing;
    this.currentTraceContext = httpTracing.tracing().currentTraceContext();
  }

  public <Builder extends AwsClientBuilder, Client> Client build(
      AwsClientBuilder<Builder, Client> builder
  ) {
    if (builder == null) throw new NullPointerException("builder == null");
    if (builder instanceof AwsAsyncClientBuilder) {
      ExecutorFactory executorFactory = ((AwsAsyncClientBuilder) builder).getExecutorFactory();
      if (executorFactory == null) {
        ClientConfiguration clientConfiguration = builder.getClientConfiguration();
        if (clientConfiguration == null) {
          clientConfiguration = defaultClientConfigurationFactory.getConfig();
        }
        ((AwsAsyncClientBuilder) builder).setExecutorFactory(
            new TracingExecutorFactory(currentTraceContext, clientConfiguration)
        );
      } else {
        ((AwsAsyncClientBuilder) builder).setExecutorFactory(
            new TracingExecutorFactoryWrapper(currentTraceContext, executorFactory)
        );
      }
    }
    builder.withRequestHandlers(new TracingRequestHandler(httpTracing));
    return builder.build();
  }

  static final class TracingExecutorFactory implements ExecutorFactory {
    final CurrentTraceContext currentTraceContext;
    final ExecutorService executorService;

    TracingExecutorFactory(
        CurrentTraceContext currentTraceContext,
        ClientConfiguration clientConfiguration
    ) {
      this.currentTraceContext = currentTraceContext;
      // same as AwsAsyncClientBuilder.AsyncBuilderParams.defaultExecutor()
      this.executorService = Executors.newFixedThreadPool(clientConfiguration.getMaxConnections());
    }

    @Override public ExecutorService newExecutor() {
      return currentTraceContext.executorService(executorService);
    }
  }

  static final class TracingExecutorFactoryWrapper implements ExecutorFactory {
    final CurrentTraceContext currentTraceContext;
    final ExecutorFactory delegate;

    TracingExecutorFactoryWrapper(
        CurrentTraceContext currentTraceContext,
        ExecutorFactory delegate
    ) {
      this.currentTraceContext = currentTraceContext;
      this.delegate = delegate;
    }

    @Override public ExecutorService newExecutor() {
      return currentTraceContext.executorService(delegate.newExecutor());
    }
  }
}
