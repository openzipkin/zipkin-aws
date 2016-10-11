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
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin.Component;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Util;
import zipkin.storage.StorageComponent;

import static zipkin.internal.Util.checkArgument;


public final class SQSCollector implements CollectorComponent, Closeable {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder implements CollectorComponent.Builder {

    Collector.Builder delegate = Collector.builder(SQSCollector.class);

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
      checkArgument(parallelism > 0 && parallelism < 21, "waitTimeSeconds");
      this.waitTimeSeconds = seconds;
      return this;
    }

    /** How many processors to run in parallel for each queue URL */
    public Builder parallelism(int parallelism) {
      checkArgument(parallelism > 0, "parallelism");
      this.parallelism = parallelism;
      return this;
    }

    @Override public SQSCollector build() {
      return new SQSCollector(this);
    }

    Builder() {
    }
  }

  private final LazyAmazonSQSAsync client;
  private final LazyProcessors processors;

  SQSCollector(Builder builder) {
    client = new LazyAmazonSQSAsync(builder.credentialsProvider,
        new QueueBufferConfig().withLongPollWaitTimeoutSeconds(builder.waitTimeSeconds));

    processors = new LazyProcessors(client.get(), builder.delegate.build(), builder.queueUrl,
        builder.parallelism, builder.waitTimeSeconds);
  }

  @Override public SQSCollector start() {
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

  private static final class LazyProcessors extends LazyCloseable<List<SQSSpanProcessor>>
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

    @Override protected List<SQSSpanProcessor> compute() {
      List<SQSSpanProcessor> processors = new LinkedList<>();
      for (int i=0; i<parallelism; i++) {
        processors.add(new SQSSpanProcessor(client, collector, queueUrl, waitTimeSeconds).run());
      }
      return processors;
    }

    @Override public CheckResult check() {
      List<SQSSpanProcessor> processors = maybeNull();
      if (processors != null) {
        for (SQSSpanProcessor processor : processors) {
          CheckResult result = processor.check();
          if (result != CheckResult.OK) return result;
        }
      }
      return CheckResult.OK;
    }

    @Override public void close() {
      List<SQSSpanProcessor> processors = maybeNull();
      if (processors == null) return;

      for (SQSSpanProcessor processor : processors) {
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
    final QueueBufferConfig config;

    LazyAmazonSQSAsync(AWSCredentialsProvider credentialsProvider, QueueBufferConfig config) {
      this.credentialsProvider = credentialsProvider;
      this.config = config;
    }

    @Override protected AmazonSQSAsync compute() {
      return new AmazonSQSBufferedAsyncClient(
          new AmazonSQSAsyncClient(credentialsProvider), config);
    }

    @Override public void close() {
      AmazonSQS client = maybeNull();
      if (client == null) return;

      client.shutdown();
    }
  }

}
