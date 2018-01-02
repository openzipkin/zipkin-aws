/**
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
package zipkin.reporter.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.Base64;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Future;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;
import zipkin2.internal.Nullable;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Sender;

/**
 * Zipkin Sender implementation that sends spans to an SQS queue.
 *
 * <p>The {@link AsyncReporter} batches spans into a single message to improve throughput and lower
 * API requests to SQS. Based on current service capabilities, a message will contain roughly 256KiB
 * of spans.
 *
 * <p>This sends (usually TBinaryProtocol big-endian) encoded spans to an SQS queue.
 */
@AutoValue
public abstract class SQSSender extends Sender {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  public static SQSSender create(String url) {
    return builder().queueUrl(url).build();
  }

  public static Builder builder() {
    return new AutoValue_SQSSender.Builder()
        .credentialsProvider(new DefaultAWSCredentialsProviderChain())
        .encoding(Encoding.JSON)
        .messageMaxBytes(256 * 1024); // 256KB SQS limit.
  }

  @AutoValue.Builder
  public interface Builder {

    /** SQS queue URL to send spans. */
    Builder queueUrl(String queueUrl);

    /** Endpoint and signing configuration for SQS. */
    Builder endpointConfiguration(EndpointConfiguration endpointConfiguration);

    /** AWS credentials for authenticating calls to SQS. */
    Builder credentialsProvider(AWSCredentialsProvider credentialsProvider);

    /** Maximum size of a message. SQS max message size is 256KB including attributes. */
    Builder messageMaxBytes(int messageMaxBytes);

    /** Controls reporting format. Currently supports json */
    Builder encoding(Encoding encoding);

    SQSSender build();
  }

  public abstract Builder toBuilder();

  abstract String queueUrl();

  @Nullable abstract AWSCredentialsProvider credentialsProvider();

  // Needed to be able to overwrite for tests
  @Nullable abstract EndpointConfiguration endpointConfiguration();

  @Override public CheckResult check() {
    // TODO need to do something better here.
    return CheckResult.OK;
  }

  /** get and close are typically called from different threads */
  volatile boolean provisioned, closeCalled;

  @Memoized AmazonSQSAsync get() {
    AmazonSQSAsync result = AmazonSQSAsyncClientBuilder.standard()
        .withCredentials(credentialsProvider())
        .withEndpointConfiguration(endpointConfiguration()).build();
    provisioned = true;
    return result;
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    int listSize = encoding().listSizeInBytes(encodedSpans);
    return (listSize + 2) * 4 / 3; // account for base64 encoding
  }

  @Override public Call<Void> sendSpans(List<byte[]> list) {
    if (closeCalled) throw new IllegalStateException("closed");

    byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);
    String body = encoding() == Encoding.JSON && isAscii(encodedSpans)
        ? new String(encodedSpans, UTF_8)
        : Base64.encodeAsString(encodedSpans);

    return new SQSCall(new SendMessageRequest(queueUrl(), body));
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    if (provisioned) get().shutdown();
    closeCalled = true;
  }

  SQSSender() {
  }

  static boolean isAscii(byte[] encodedSpans) {
    for (int i = 0; i < encodedSpans.length; i++) {
      if (encodedSpans[i] >= 0x80) {
        return false;
      }
    }
    return true;
  }

  class SQSCall extends Call.Base<Void> {
    private final SendMessageRequest message;
    transient Future<SendMessageResult> future;

    SQSCall(SendMessageRequest message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      get().sendMessage(message);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      future = get().sendMessageAsync(message,
          new AsyncHandler<SendMessageRequest, SendMessageResult>() {
            @Override public void onError(Exception e) {
              callback.onError(e);
            }

            @Override
            public void onSuccess(SendMessageRequest request, SendMessageResult sendMessageResult) {
              callback.onSuccess(null);
            }
          });
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
}
