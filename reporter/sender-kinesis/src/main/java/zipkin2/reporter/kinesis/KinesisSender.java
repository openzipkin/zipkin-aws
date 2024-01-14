/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
package zipkin2.reporter.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
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
    AWSCredentialsProvider credentialsProvider;
    EndpointConfiguration endpointConfiguration;
    int messageMaxBytes = 1024 * 1024; // 1MB Kinesis limit.
    Encoding encoding = Encoding.JSON;

    Builder(KinesisSender sender) {
      this.streamName = sender.streamName;
      this.region = sender.region;
      this.credentialsProvider = sender.credentialsProvider;
      this.endpointConfiguration = sender.endpointConfiguration;
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
    public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
      if (credentialsProvider == null) {
        throw new NullPointerException("credentialsProvider == null");
      }
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /** Endpoint and signing configuration for Kinesis. */
    public Builder endpointConfiguration(EndpointConfiguration endpointConfiguration) {
      if (endpointConfiguration == null) {
        throw new NullPointerException("endpointConfiguration == null");
      }
      this.endpointConfiguration = endpointConfiguration;
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
  @Nullable final AWSCredentialsProvider credentialsProvider;
  @Nullable final EndpointConfiguration endpointConfiguration;
  final int messageMaxBytes;

  KinesisSender(Builder builder) {
    super(builder.encoding);
    this.streamName = builder.streamName;
    this.region = builder.region;
    this.credentialsProvider = builder.credentialsProvider;
    this.endpointConfiguration = builder.endpointConfiguration;
    this.messageMaxBytes = builder.messageMaxBytes;
  }

  private final AtomicReference<String> partitionKey = new AtomicReference<>("");

  private String getPartitionKey() {
    if (partitionKey.get().isEmpty()) {
      try {
        partitionKey.set(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        // Shouldn't be possible, but you know
        partitionKey.set(UUID.randomUUID().toString());
      }
    }
    return partitionKey.get();
  }

  /** get and close are typically called from different threads */
  volatile AmazonKinesis client;
  volatile boolean closeCalled;

  AmazonKinesis get() {
    if (client == null) {
      synchronized (this) {
        if (client != null) return client;
        AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withEndpointConfiguration(endpointConfiguration);
        if (region != null) builder.withRegion(region);
        client = builder.build();
      }
    }
    return client;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public void send(List<byte[]> list) {
    if (closeCalled) throw new ClosedSenderException();

    ByteBuffer message = ByteBuffer.wrap(BytesMessageEncoder.forEncoding(encoding()).encode(list));

    PutRecordRequest request = new PutRecordRequest();
    request.setStreamName(streamName);
    request.setData(message);
    request.setPartitionKey(getPartitionKey());

    get().putRecord(request);
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    AmazonKinesis client = this.client;
    if (client != null) client.shutdown();
    closeCalled = true;
  }
}
