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
package zipkin2.reporter.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.Base64;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Future;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;
import zipkin2.reporter.internal.Nullable;

/**
 * Zipkin Sender implementation that sends spans to an SQS queue.
 *
 * <p>The {@link AsyncReporter} batches spans into a single message to improve throughput and lower
 * API requests to SQS. Based on current service capabilities, a message will contain roughly 256KiB
 * of spans.
 *
 * <p>This sends (usually TBinaryProtocol big-endian) encoded spans to an SQS queue.
 */
public final class SQSSender extends Sender {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  public static SQSSender create(String url) {
    return newBuilder().queueUrl(url).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    String queueUrl;
    EndpointConfiguration endpointConfiguration;
    AWSCredentialsProvider credentialsProvider;
    int messageMaxBytes = 256 * 1024; // 256KB SQS limit
    Encoding encoding = Encoding.JSON;

    Builder(SQSSender sender) {
      this.queueUrl = sender.queueUrl;
      this.credentialsProvider = sender.credentialsProvider;
      this.endpointConfiguration = sender.endpointConfiguration;
      this.messageMaxBytes = sender.messageMaxBytes;
      this.encoding = sender.encoding;
    }

    /** SQS queue URL to send spans. */
    public Builder queueUrl(String queueUrl) {
      if (queueUrl == null) throw new NullPointerException("queueUrl == null");
      this.queueUrl = queueUrl;
      return this;
    }

    /** AWS credentials for authenticating calls to SQS. */
    public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
      if (credentialsProvider == null) {
        throw new NullPointerException("credentialsProvider == null");
      }
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /** Endpoint and signing configuration for SQS. */
    public Builder endpointConfiguration(EndpointConfiguration endpointConfiguration) {
      if (endpointConfiguration == null) {
        throw new NullPointerException("endpointConfiguration == null");
      }
      this.endpointConfiguration = endpointConfiguration;
      return this;
    }

    /** Maximum size of a message. SQS max message size is 256KB including attributes. */
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

    public SQSSender build() {
      if (queueUrl == null) throw new NullPointerException("queueUrl == null");
      return new SQSSender(this);
    }

    Builder() {
    }
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  final String queueUrl;
  @Nullable final AWSCredentialsProvider credentialsProvider;
  @Nullable final EndpointConfiguration endpointConfiguration;
  final int messageMaxBytes;
  final Encoding encoding;

  SQSSender(Builder builder) {
    this.queueUrl = builder.queueUrl;
    this.credentialsProvider = builder.credentialsProvider;
    this.endpointConfiguration = builder.endpointConfiguration;
    this.messageMaxBytes = builder.messageMaxBytes;
    this.encoding = builder.encoding;
  }

  @Override public CheckResult check() {
    // TODO need to do something better here.
    return CheckResult.OK;
  }

  /** get and close are typically called from different threads */
  volatile AmazonSQSAsync asyncClient;
  volatile boolean closeCalled;

  AmazonSQSAsync get() {
    if (asyncClient == null) {
      synchronized (this) {
        if (asyncClient == null) {
          asyncClient = AmazonSQSAsyncClientBuilder.standard()
              .withCredentials(credentialsProvider)
              .withEndpointConfiguration(endpointConfiguration).build();
        }
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

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    int listSize = encoding().listSizeInBytes(encodedSpans);
    return (listSize + 2) * 4 / 3; // account for base64 encoding
  }

  @Override
  public Call<Void> sendSpans(List<byte[]> list) {
    if (closeCalled) throw new IllegalStateException("closed");

    byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);
    String body =
        encoding() == Encoding.JSON && isAscii(encodedSpans)
            ? new String(encodedSpans, UTF_8)
            : Base64.encodeAsString(encodedSpans);

    return new SQSCall(new SendMessageRequest(queueUrl, body));
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    AmazonSQSAsync asyncClient = this.asyncClient;
    if (asyncClient != null) asyncClient.shutdown();
    closeCalled = true;
  }

  @Override public final String toString() {
    return "SQSSender{queueUrl=" + queueUrl + "}";
  }

  static boolean isAscii(byte[] encodedSpans) {
    for (int i = 0; i < encodedSpans.length; i++) {
      if (Byte.toUnsignedInt(encodedSpans[i]) >= 0x80) {
        return false;
      }
    }
    return true;
  }

  class SQSCall extends Call.Base<Void> {
    private final SendMessageRequest message;
    volatile Future<SendMessageResult> future;

    SQSCall(SendMessageRequest message) {
      this.message = message;
    }

    @Override
    protected Void doExecute() {
      get().sendMessage(message);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      future = get().sendMessageAsync(message, new AsyncHandlerAdapter(callback));
      if (future.isCancelled()) throw new IllegalStateException("cancelled sending spans");
    }

    @Override protected void doCancel() {
      Future<SendMessageResult> maybeFuture = future;
      if (maybeFuture != null) maybeFuture.cancel(true);
    }

    @Override protected boolean doIsCanceled() {
      Future<SendMessageResult> maybeFuture = future;
      return maybeFuture != null && maybeFuture.isCancelled();
    }

    @Override public Call<Void> clone() {
      return new SQSCall(message.clone());
    }
  }

  static final class AsyncHandlerAdapter
      implements AsyncHandler<SendMessageRequest, SendMessageResult> {
    final Callback<Void> callback;

    AsyncHandlerAdapter(Callback<Void> callback) {
      this.callback = callback;
    }

    @Override public void onError(Exception e) {
      callback.onError(e);
    }

    @Override public void onSuccess(SendMessageRequest request, SendMessageResult result) {
      callback.onSuccess(null);
    }
  }
}
