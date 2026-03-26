/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.awssdk.kinesis;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.internal.Nullable;

public final class KinesisSender extends BytesMessageSender.Base {

  public static KinesisSender create(String streamName) {
    return newBuilder().streamName(streamName).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    String streamName, region;
    AwsCredentialsProvider credentialsProvider;
    URI endpointOverride;
    KinesisClient kinesisClient;
    int messageMaxBytes = 1024 * 1024; // 1MB Kinesis limit.
    Encoding encoding = Encoding.JSON;

    Builder(KinesisSender sender) {
      this.streamName = sender.streamName;
      this.region = sender.region;
      this.credentialsProvider = sender.credentialsProvider;
      this.endpointOverride = sender.endpointOverride;
      this.kinesisClient = sender.providedClient;
      this.messageMaxBytes = sender.messageMaxBytes;
      this.encoding = sender.encoding;
    }

    /** Kinesis stream to send spans. */
    public Builder streamName(String streamName) {
      if (streamName == null) throw new NullPointerException("streamName == null");
      this.streamName = streamName;
      return this;
    }

    public Builder region(String region) {
      if (region == null) throw new NullPointerException("region == null");
      this.region = region;
      return this;
    }

    /** AWS credentials for authenticating calls to Kinesis. */
    public Builder credentialsProvider(AwsCredentialsProvider credentialsProvider) {
      if (credentialsProvider == null) {
        throw new NullPointerException("credentialsProvider == null");
      }
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /** Endpoint override for Kinesis. */
    public Builder endpointOverride(URI endpointOverride) {
      if (endpointOverride == null) {
        throw new NullPointerException("endpointOverride == null");
      }
      this.endpointOverride = endpointOverride;
      return this;
    }

    /** Use a pre-built {@link KinesisClient}. */
    public Builder kinesisClient(KinesisClient kinesisClient) {
      if (kinesisClient == null) throw new NullPointerException("kinesisClient == null");
      this.kinesisClient = kinesisClient;
      return this;
    }

    /** Maximum size of a message. Kinesis max message size is 1MB */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    /**
     * Use this to change the encoding used in messages. Default is {@linkplain Encoding#JSON}
     *
     * <p>Note: If ultimately sending to Zipkin, version 2.8+ is required to process protobuf.
     */
    public Builder encoding(Encoding encoding) {
      if (encoding == null) throw new NullPointerException("encoding == null");
      this.encoding = encoding;
      return this;
    }

    public KinesisSender build() {
      if (streamName == null) throw new NullPointerException("streamName == null");
      return new KinesisSender(this);
    }

    Builder() {
    }
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  final String streamName;
  @Nullable final String region;
  @Nullable final AwsCredentialsProvider credentialsProvider;
  @Nullable final URI endpointOverride;
  @Nullable final KinesisClient providedClient;
  final int messageMaxBytes;

  KinesisSender(Builder builder) {
    super(builder.encoding);
    this.streamName = builder.streamName;
    this.region = builder.region;
    this.credentialsProvider = builder.credentialsProvider;
    this.endpointOverride = builder.endpointOverride;
    this.providedClient = builder.kinesisClient;
    this.messageMaxBytes = builder.messageMaxBytes;
  }

  private final AtomicReference<String> partitionKey = new AtomicReference<>("");

  private String getPartitionKey() {
    if (partitionKey.get().isEmpty()) {
      try {
        partitionKey.set(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        partitionKey.set(UUID.randomUUID().toString());
      }
    }
    return partitionKey.get();
  }

  /** get and close are typically called from different threads */
  volatile KinesisClient client;
  volatile boolean closeCalled;

  KinesisClient get() {
    if (client == null) {
      synchronized (this) {
        if (client != null) return client;
        if (providedClient != null) {
          client = providedClient;
        } else {
          KinesisClientBuilder builder = KinesisClient.builder();
          if (credentialsProvider != null) builder.credentialsProvider(credentialsProvider);
          if (endpointOverride != null) builder.endpointOverride(endpointOverride);
          if (region != null) builder.region(Region.of(region));
          client = builder.build();
        }
      }
    }
    return client;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public void send(List<byte[]> list) {
    if (closeCalled) throw new ClosedSenderException();

    byte[] message = BytesMessageEncoder.forEncoding(encoding()).encode(list);

    get().putRecord(PutRecordRequest.builder()
        .streamName(streamName)
        .data(SdkBytes.fromByteArray(message))
        .partitionKey(getPartitionKey())
        .build());
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    KinesisClient client = this.client;
    if (client != null && providedClient == null) client.close();
    closeCalled = true;
  }
}
