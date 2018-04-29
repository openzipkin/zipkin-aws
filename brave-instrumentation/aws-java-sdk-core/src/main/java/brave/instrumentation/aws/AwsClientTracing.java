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
