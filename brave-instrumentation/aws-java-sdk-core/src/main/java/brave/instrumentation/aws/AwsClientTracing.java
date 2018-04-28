package brave.instrumentation.aws;

import brave.http.HttpTracing;
import com.amazonaws.client.builder.AwsAsyncClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.ExecutorFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public final class AwsClientTracing {

  public static <Builder extends AwsClientBuilder, Client> Client build(HttpTracing httpTracing, AwsClientBuilder<Builder, Client> awsClientBuilder) {
    if (awsClientBuilder instanceof AwsAsyncClientBuilder) {
      ExecutorFactory executorFactory = ((AwsAsyncClientBuilder) awsClientBuilder).getExecutorFactory();
      if (executorFactory == null) {
        ((AwsAsyncClientBuilder) awsClientBuilder).setExecutorFactory(new TracingExecutorFactory(httpTracing));
      } else {
        ((AwsAsyncClientBuilder) awsClientBuilder).setExecutorFactory(
            new TracingExecutorFactoryWrapper(executorFactory, httpTracing));
      }
    }

    awsClientBuilder.withRequestHandlers(new TracingRequestHandler(httpTracing));
    return awsClientBuilder.build();
  }

  static final class TracingExecutorFactory implements ExecutorFactory {
    private HttpTracing httpTracing;
    private ExecutorService executorService = new ForkJoinPool();

    TracingExecutorFactory(HttpTracing httpTracing) {
      this.httpTracing = httpTracing;
    }

    @Override public ExecutorService newExecutor() {
      return httpTracing.tracing().currentTraceContext().executorService(executorService);
    }
  }

  static final class TracingExecutorFactoryWrapper implements ExecutorFactory {
    private ExecutorFactory delegate;
    private HttpTracing httpTracing;

    TracingExecutorFactoryWrapper(ExecutorFactory delegate, HttpTracing httpTracing) {
      this.delegate = delegate;
      this.httpTracing = httpTracing;
    }

    @Override
    public ExecutorService newExecutor() {
      return httpTracing.tracing().currentTraceContext().executorService(delegate.newExecutor());
    }
  }

}
