/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin.collector.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.LazyCloseable;
import zipkin.storage.StorageComponent;

public final class SQSCollector implements CollectorComponent, Closeable {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder implements CollectorComponent.Builder {

    Collector.Builder delegate = Collector.builder(SQSCollector.class);

    String queueUrl;
    int waitTimeSeconds = 20; // aws sqs max wait time is 20 seconds
    int maxNumberOfMessages = 10; // aws sqs max messages for a receive call is 10
    EndpointConfiguration endpointConfiguration;
    AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    int parallelism = 1;

    @Override public Builder storage(StorageComponent storageComponent) {
      delegate.storage(storageComponent);
      return this;
    }

    @Override public Builder metrics(CollectorMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      delegate.metrics(metrics.forTransport("sqs"));
      return this;
    }

    @Override public Builder sampler(CollectorSampler sampler) {
      delegate.sampler(sampler);
      return this;
    }

    /** SQS queue URL to consume from */
    public Builder queueUrl(String queueUrl) {
      this.queueUrl = queueUrl;
      return this;
    }

    /** Endpoint and signing configuration for SQS. */
    public Builder endpointConfiguration(EndpointConfiguration endpointConfiguration){
      this.endpointConfiguration = endpointConfiguration;
      return this;
    }

    /** AWS credentials for authenticating calls to SQS. */
    public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /** Amount of time to wait for messages from SQS */
    public Builder waitTimeSeconds(int seconds) {
      if (seconds < 1 || seconds > 20) {
        throw new IllegalArgumentException("waitTimeSeconds must be between 1 and 20");
      }
      this.waitTimeSeconds = seconds;
      return this;
    }

    /** Maximum number of messages to retrieve per API call to SQS */
    public Builder maxNumberOfMessages(int maxNumberOfMessages) {
      if (maxNumberOfMessages < 1 || maxNumberOfMessages > 10) {
        throw new IllegalArgumentException("maxNumberOfMessages must be between 1 and 10");
      }
      this.maxNumberOfMessages = maxNumberOfMessages;
      return this;
    }

    /** How many processors to run in parallel for each queue URL */
    public Builder parallelism(int parallelism) {
      if (parallelism < 1) throw new IllegalArgumentException("parallelism must be positive");
      this.parallelism = parallelism;
      return this;
    }

    @Override public SQSCollector build() {
      return new SQSCollector(this);
    }

    Builder() {
    }
  }

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final LazyAmazonSQSClient client;
  private final List<SQSSpanProcessor> processors = new LinkedList<>();
  private final ExecutorService pool;
  private final int parallelism;
  private final int waitTimeSeconds;
  private final int maxNumberOfMessages;
  private final String queueUrl;
  private final Collector collector;

  SQSCollector(Builder builder) {
    client = new LazyAmazonSQSClient(builder);
    collector = builder.delegate.build();
    parallelism = builder.parallelism;
    waitTimeSeconds = builder.waitTimeSeconds;
    maxNumberOfMessages = builder.maxNumberOfMessages;
    queueUrl = builder.queueUrl;

    pool = (builder.parallelism == 1)
        ? Executors.newSingleThreadExecutor()
        : Executors.newFixedThreadPool(builder.parallelism);
  }

  @Override public SQSCollector start() {
    if (!closed.get()) {
      for (int i=0; i<parallelism; i++) {
        SQSSpanProcessor processor = new SQSSpanProcessor(
            client.get(), collector, queueUrl,
            waitTimeSeconds, maxNumberOfMessages, closed);

        Future<?> task = pool.submit(processor);
        if (task.isDone()) throw new IllegalStateException("processor quit " + processor);
        processors.add(processor);
      }
    }
    return this;
  }

  @Override public CheckResult check() {
    try {
      client.get(); // make sure compute doesn't throw an exception
      for (SQSSpanProcessor processor : processors) { // check if any processor have failed
        if (!processor.check().equals(CheckResult.OK)) {
          return processor.check();
        }
      }
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() throws IOException {
    try {
      if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {}
    finally {
      pool.shutdownNow();
      client.close();
    }
  }

  private static final class LazyAmazonSQSClient extends LazyCloseable<AmazonSQS> {

    final AmazonSQSClientBuilder builder;

    LazyAmazonSQSClient(Builder builder) {
      this.builder = AmazonSQSClientBuilder.standard()
          .withEndpointConfiguration(builder.endpointConfiguration)
          .withCredentials(builder.credentialsProvider);
    }

    @Override protected AmazonSQS compute() {
      return builder.build();
    }

    @Override public void close() {
      AmazonSQS client = maybeNull();
      if (client == null) return;

      client.shutdown();
    }
  }

}
