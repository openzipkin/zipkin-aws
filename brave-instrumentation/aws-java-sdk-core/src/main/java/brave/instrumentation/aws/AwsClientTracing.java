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

import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsAsyncClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.ExecutorFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class AwsClientTracing<Builder extends AwsClientBuilder, Client> {

  HttpTracing httpTracing;
  CurrentTraceContext currentTraceContext;
  AwsClientBuilder<Builder, Client> awsClientBuilder;

  public AwsClientTracing() {
  }

  public AwsClientTracing<Builder, Client> withHttpTracing(HttpTracing httpTracing) {
    this.httpTracing = httpTracing;
    return this;
  }

  public AwsClientTracing<Builder, Client> withCurrentTraceContext(CurrentTraceContext currentTraceContext) {
    this.currentTraceContext = currentTraceContext;
    return this;
  }

  public AwsClientTracing<Builder, Client> withAwsClientBuilder(AwsClientBuilder awsClientBuilder) {
    this.awsClientBuilder = awsClientBuilder;
    return this;
  }

  public Client build() {
    if (httpTracing == null || currentTraceContext == null || awsClientBuilder == null) {
      throw new IllegalStateException("AwsClientTracing expects All of: HttpTracing, CurrentTraceContext, and AwsClientBuilder");
    }
    if (awsClientBuilder instanceof AwsAsyncClientBuilder) {
      ExecutorFactory executorFactory = ((AwsAsyncClientBuilder) awsClientBuilder).getExecutorFactory();
      if (executorFactory == null) {
        ((AwsAsyncClientBuilder) awsClientBuilder).setExecutorFactory(new TracingExecutorFactory(currentTraceContext, awsClientBuilder.getClientConfiguration()));
      } else {
        ((AwsAsyncClientBuilder) awsClientBuilder).setExecutorFactory(
            new TracingExecutorFactoryWrapper(currentTraceContext, executorFactory));
      }
    }

    awsClientBuilder.withRequestHandlers(new TracingRequestHandler(httpTracing));
    return awsClientBuilder.build();
  }

  static final class TracingExecutorFactory implements ExecutorFactory {
    CurrentTraceContext currentTraceContext;
    ExecutorService executorService;

    TracingExecutorFactory(CurrentTraceContext currentTraceContext, ClientConfiguration clientConfiguration) {
      this.currentTraceContext = currentTraceContext;
      this.executorService = Executors.newFixedThreadPool(clientConfiguration.getMaxConnections());
    }

    @Override public ExecutorService newExecutor() {
      return currentTraceContext.executorService(executorService);
    }
  }

  static final class TracingExecutorFactoryWrapper implements ExecutorFactory {
    CurrentTraceContext currentTraceContext;
    ExecutorFactory delegate;

    TracingExecutorFactoryWrapper(CurrentTraceContext currentTraceContext, ExecutorFactory delegate) {
      this.currentTraceContext = currentTraceContext;
      this.delegate = delegate;
    }

    @Override
    public ExecutorService newExecutor() {
      return currentTraceContext.executorService(delegate.newExecutor());
    }
  }

}
