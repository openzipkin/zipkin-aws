/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.collector.sqs;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import zipkin2.CheckResult;
import zipkin2.collector.Collector;
import zipkin2.collector.CollectorComponent;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.CollectorSampler;
import zipkin2.storage.StorageComponent;

public final class SQSCollector extends CollectorComponent {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends CollectorComponent.Builder {

    Collector.Builder delegate = Collector.newBuilder(SQSCollector.class);
    CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;

    String queueUrl;
    int waitTimeSeconds = 20; // aws sqs max wait time is 20 seconds
    int maxNumberOfMessages = 10; // aws sqs max messages for a receive call is 10
    URI endpointOverride;
    String region = "us-east-1";
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    int parallelism = 1;

    @Override
    public Builder storage(StorageComponent storageComponent) {
      delegate.storage(storageComponent);
      return this;
    }

    @Override
    public Builder metrics(CollectorMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      delegate.metrics(this.metrics = metrics.forTransport("sqs"));
      return this;
    }

    @Override
    public Builder sampler(CollectorSampler sampler) {
      delegate.sampler(sampler);
      return this;
    }

    /** SQS queue URL to consume from */
    public Builder queueUrl(String queueUrl) {
      this.queueUrl = queueUrl;
      return this;
    }

    /** Endpoint override for SQS. */
    public Builder endpointOverride(URI endpointOverride) {
      this.endpointOverride = endpointOverride;
      return this;
    }

    /** AWS region for SQS. */
    public Builder region(String region) {
      this.region = region;
      return this;
    }

    /** AWS credentials for authenticating calls to SQS. */
    public Builder credentialsProvider(AwsCredentialsProvider credentialsProvider) {
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

    @Override
    public SQSCollector build() {
      return new SQSCollector(this);
    }

    Builder() {}
  }

  final AtomicBoolean closed = new AtomicBoolean(false);
  final LazySqsClient client;
  final List<SQSSpanProcessor> processors = new ArrayList<>();
  final ExecutorService pool;
  final int parallelism;
  final int waitTimeSeconds;
  final int maxNumberOfMessages;
  final String queueUrl;
  final Collector collector;
  final CollectorMetrics metrics;

  SQSCollector(Builder builder) {
    client = new LazySqsClient(builder);
    collector = builder.delegate.build();
    metrics = builder.metrics;
    parallelism = builder.parallelism;
    waitTimeSeconds = builder.waitTimeSeconds;
    maxNumberOfMessages = builder.maxNumberOfMessages;
    queueUrl = builder.queueUrl;

    pool =
        (builder.parallelism == 1)
            ? Executors.newSingleThreadExecutor()
            : Executors.newFixedThreadPool(builder.parallelism);
  }

  @Override
  public SQSCollector start() {
    if (!closed.get()) {
      for (int i = 0; i < parallelism; i++) {
        SQSSpanProcessor processor = new SQSSpanProcessor(this);
        Future<?> task = pool.submit(processor);
        if (task.isDone()) throw new IllegalStateException("processor quit " + processor);
        processors.add(processor);
      }
    }
    return this;
  }

  @Override
  public CheckResult check() {
    try {
      client(); // make sure compute doesn't throw an exception
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

  SqsClient client() {
    return client.get();
  }

  @Override
  public void close() {
    try {
      if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
    } finally {
      pool.shutdownNow();
      client.close();
    }
  }

  private static final class LazySqsClient {
    final AwsCredentialsProvider credentialsProvider;
    final URI endpointOverride;
    final String region;
    volatile SqsClient client;

    LazySqsClient(Builder builder) {
      this.credentialsProvider = builder.credentialsProvider;
      this.endpointOverride = builder.endpointOverride;
      this.region = builder.region;
    }

    SqsClient get() {
      if (client == null) {
        synchronized (this) {
          if (client == null) {
            SqsClientBuilder builder = SqsClient.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .credentialsProvider(credentialsProvider);
            if (endpointOverride != null) builder.endpointOverride(endpointOverride);
            if (region != null) builder.region(Region.of(region));
            client = builder.build();
          }
        }
      }
      return client;
    }

    void close() {
      SqsClient maybeClient = client;
      if (maybeClient == null) return;
      maybeClient.close();
    }
  }
}
