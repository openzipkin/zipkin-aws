/**
 * Copyright 2016 The OpenZipkin Authors
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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import zipkin.Component;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Util;
import zipkin.storage.StorageComponent;
import static zipkin.internal.Util.*;


public final class AwsSqsCollector implements CollectorComponent, Closeable {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder implements CollectorComponent.Builder {

    Collector.Builder delegate = Collector.builder(AwsSqsCollector.class);

    String queueUrl;
    int waitTimeSeconds = 20; // aws sqs max wait time is 20 seconds
    AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    int parallelism = 1;

    @Override public Builder storage(StorageComponent storageComponent) {
      delegate.storage(storageComponent);
      return this;
    }

    @Override public Builder metrics(CollectorMetrics metrics) {
      delegate.metrics(Util.checkNotNull(metrics, "metrics").forTransport("sqs"));
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

    /** AWS credentials for authenticating calls to SQS. */
    public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /** Amount of time to wait for messages from SQS */
    public Builder waitTimeSeconds(int seconds) {
      checkArgument(parallelism > 0 && parallelism < 21, "parallelism");
      this.waitTimeSeconds = seconds;
      return this;
    }

    /** How many processors to run in parallel for each queue URL */
    public Builder parallelism(int parallelism) {
      checkArgument(parallelism > 0, "parallelism");
      this.parallelism = parallelism;
      return this;
    }

    @Override public AwsSqsCollector build() {
      return new AwsSqsCollector(this);
    }

    Builder() {
    }
  }

  private final LazyAmazonSQSAsync client;
  private final LazyProcessors processors;

  AwsSqsCollector(Builder builder) {
    client = new LazyAmazonSQSAsync(builder.credentialsProvider);
    processors = new LazyProcessors(client.get(), builder.delegate.build(), builder.queueUrl,
        builder.parallelism, builder.waitTimeSeconds);
  }

  @Override public AwsSqsCollector start() {
    processors.get();
    return this;
  }

  @Override public CheckResult check() {
    try {
      client.get(); // make sure compute doesn't throw an exception
      return processors.check(); // check if any processor has failed
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() throws IOException {
    client.close();
    processors.close();
  }

  private static final class LazyProcessors extends LazyCloseable<List<AwsSqsSpanProcessor>>
      implements Component {

    private static final Logger logger = Logger.getLogger(LazyProcessors.class.getName());

    final String queueUrl;
    final int parallelism;
    final AmazonSQSAsync client;
    final Collector collector;
    final int waitTimeSeconds;

    LazyProcessors(AmazonSQSAsync client, Collector collector, String queueUrl, int parallelism, int waitTimeSeconds) {
      this.queueUrl = queueUrl;
      this.parallelism = parallelism;
      this.client = client;
      this.collector = collector;
      this.waitTimeSeconds = waitTimeSeconds;
    }

    @Override protected List<AwsSqsSpanProcessor> compute() {
      return IntStream.range(0, parallelism - 1)
          .mapToObj(i -> new AwsSqsSpanProcessor(client, collector, queueUrl, waitTimeSeconds).run())
          .collect(Collectors.toList());
    }

    @Override public CheckResult check() {
      List<AwsSqsSpanProcessor> processors = maybeNull();
      if (processors != null) {
        for (AwsSqsSpanProcessor processor : processors) {
          CheckResult result = processor.check();
          if (result != CheckResult.OK) return result;
        }
      }
      return CheckResult.OK;
    }

    @Override public void close() {
      List<AwsSqsSpanProcessor> processors = maybeNull();
      if (processors == null) return;

      for (AwsSqsSpanProcessor processor : processors) {
        try {
          processor.close();
        } catch (IOException ioe) {
          logger.log(Level.WARNING, "Processor failed to close cleanly", ioe);
        }
      }
    }
  }

  private static final class LazyAmazonSQSAsync extends LazyCloseable<AmazonSQSAsync> {

    final AWSCredentialsProvider credentialsProvider;

    LazyAmazonSQSAsync(AWSCredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
    }

    @Override protected AmazonSQSAsync compute() {
      return new AmazonSQSBufferedAsyncClient(new AmazonSQSAsyncClient(credentialsProvider));
    }

    @Override public void close() {
      AmazonSQS client = maybeNull();
      if (client == null) return;

      client.shutdown();
    }
  }

}
