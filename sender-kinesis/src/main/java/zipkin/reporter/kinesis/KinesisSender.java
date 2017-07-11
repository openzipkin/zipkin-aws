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
package zipkin.reporter.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.google.auto.value.AutoValue;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkArgument;

@AutoValue
public abstract class KinesisSender extends LazyCloseable<AmazonKinesisAsync> implements Sender {

  public static KinesisSender create(String streamName) {
    return builder().streamName(streamName).build();
  }

  public static Builder builder() {
    return new AutoValue_KinesisSender.Builder()
        .credentialsProvider(new DefaultAWSCredentialsProviderChain())
        .messageMaxBytes(1024 * 1024); // 1MB Kinesis limit.
  }

  @AutoValue.Builder
  public interface Builder {

    /** Kinesis stream to send spans. */
    Builder streamName(String streamName);

    Builder region(String region);

    /** AWS credentials for authenticating calls to Kinesis. */
    Builder credentialsProvider(AWSCredentialsProvider credentialsProvider);

    Builder endpointConfiguration(EndpointConfiguration endpointConfiguration);

    /** Maximum size of a message. Kinesis max message size is 1MB */
    Builder messageMaxBytes(int messageMaxBytes);

    KinesisSender build();
  }

  abstract String streamName();

  @Nullable
  abstract String region();

  @Nullable
  abstract AWSCredentialsProvider credentialsProvider();

  // Needed to be able to overwrite for tests
  @Nullable
  abstract EndpointConfiguration endpointConfiguration();

  private final AtomicBoolean closeCalled = new AtomicBoolean(false);

  private final AtomicReference<String> partitionKey = new AtomicReference<>("");

  @Override
  public CheckResult check() {
    try {
      String status = get().describeStream(streamName()).getStreamDescription().getStreamStatus();
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

  @Override
  protected AmazonKinesisAsync compute() {
    AmazonKinesisAsyncClientBuilder builder = AmazonKinesisAsyncClientBuilder.standard();
    if (credentialsProvider() != null) {
      builder.withCredentials(credentialsProvider());
    }
    if (endpointConfiguration() != null) {
      builder.withEndpointConfiguration(endpointConfiguration());
    }
    if (region() != null) {
      builder.withRegion(region());
    }
    return builder.build();
  }

  @Override
  public Encoding encoding() {
    return Encoding.THRIFT;
  }

  @Override
  public int messageSizeInBytes(List<byte[]> list) {
    return Encoding.THRIFT.listSizeInBytes(list);
  }

  @Override
  public void sendSpans(List<byte[]> list, Callback callback) {
    ByteBuffer message = ByteBuffer.wrap(BytesMessageEncoder.forEncoding(encoding()).encode(list));

    PutRecordRequest request = new PutRecordRequest();
    request.setStreamName(streamName());
    request.setData(message);
    request.setPartitionKey(getPartitionKey());

    Future<PutRecordResult> future = get().putRecordAsync(request,
        new AsyncHandler<PutRecordRequest, PutRecordResult>() {
          @Override
          public void onError(Exception e) {
            callback.onError(e);
          }

          @Override
          public void onSuccess(PutRecordRequest request, PutRecordResult putRecordResult) {
            callback.onComplete();
          }
        });
    checkArgument(!future.isCancelled(), "cancelled sending spans");
  }

  @Override
  public void close() {
    if (!closeCalled.getAndSet(true)) {
      AmazonKinesisAsync maybeNull = maybeNull();
      if (maybeNull != null) maybeNull.shutdown();
    }
  }

  KinesisSender() {
  }
}
