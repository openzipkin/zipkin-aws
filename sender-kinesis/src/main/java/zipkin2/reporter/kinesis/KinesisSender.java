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
package zipkin2.reporter.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;
import zipkin2.internal.Nullable;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Sender;

public final class KinesisSender extends Sender {

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
  final Encoding encoding;

  KinesisSender(Builder builder) {
    this.streamName = builder.streamName;
    this.region = builder.region;
    this.credentialsProvider = builder.credentialsProvider;
    this.endpointConfiguration = builder.endpointConfiguration;
    this.messageMaxBytes = builder.messageMaxBytes;
    this.encoding = builder.encoding;
  }

  private final AtomicReference<String> partitionKey = new AtomicReference<>("");

  @Override public CheckResult check() {
    try {
      String status = get().describeStream(streamName).getStreamDescription().getStreamStatus();
      if (status.equalsIgnoreCase("ACTIVE")) {
        return CheckResult.OK;
      } else {
        return CheckResult.failed(new IllegalStateException("Stream is not active"));
      }
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

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
  volatile AmazonKinesisAsync asyncClient;
  volatile boolean closeCalled;

  AmazonKinesisAsync get() {
    if (asyncClient == null) {
      synchronized (this) {
        if (asyncClient != null) return asyncClient;
        AmazonKinesisAsyncClientBuilder builder = AmazonKinesisAsyncClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withEndpointConfiguration(endpointConfiguration);
        if (region != null) builder.withRegion(region);
        asyncClient = builder.build();
      }
    }
    return asyncClient;
  }

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(List<byte[]> list) {
    return encoding().listSizeInBytes(list);
  }

  @Override public Call<Void> sendSpans(List<byte[]> list) {
    if (closeCalled) throw new IllegalStateException("closed");

    ByteBuffer message = ByteBuffer.wrap(BytesMessageEncoder.forEncoding(encoding()).encode(list));

    PutRecordRequest request = new PutRecordRequest();
    request.setStreamName(streamName);
    request.setData(message);
    request.setPartitionKey(getPartitionKey());

    return new KinesisCall(request);
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    AmazonKinesisAsync asyncClient = this.asyncClient;
    if (asyncClient != null) asyncClient.shutdown();
    closeCalled = true;
  }

  @Override public final String toString() {
    return "KinesisSender{region=" + region + ", streamName=" + streamName + "}";
  }

  class KinesisCall extends Call.Base<Void> {
    private final PutRecordRequest message;
    volatile Future<PutRecordResult> future;

    KinesisCall(PutRecordRequest message) {
      this.message = message;
    }

    @Override protected Void doExecute() {
      get().putRecord(message);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      future = get().putRecordAsync(message, new AsyncHandlerAdapter(callback));
      if (future.isCancelled()) throw new IllegalStateException("cancelled sending spans");
    }

    @Override protected void doCancel() {
      Future<PutRecordResult> maybeFuture = future;
      if (maybeFuture != null) maybeFuture.cancel(true);
    }

    @Override protected boolean doIsCanceled() {
      Future<PutRecordResult> maybeFuture = future;
      return maybeFuture != null && maybeFuture.isCancelled();
    }

    @Override public Call<Void> clone() {
      return new KinesisCall(message.clone());
    }
  }

  static final class AsyncHandlerAdapter
      implements AsyncHandler<PutRecordRequest, PutRecordResult> {
    final Callback<Void> callback;

    AsyncHandlerAdapter(Callback<Void> callback) {
      this.callback = callback;
    }

    @Override public void onError(Exception e) {
      callback.onError(e);
    }

    @Override public void onSuccess(PutRecordRequest request, PutRecordResult result) {
      callback.onSuccess(null);
    }
  }
}
