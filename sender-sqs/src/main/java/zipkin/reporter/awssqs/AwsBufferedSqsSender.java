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
package zipkin.reporter.awssqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkNotNull;

/**
 * Zipkin Sender implementation that sends spans to an SQS queue. This uses the
 * AmazonSQSBufferedAsyncClient which batches messages to improve throughput and lower API requests
 * to SQS. By default this client will send messages every 200ms or when ever at most 10 messages
 * are buffered. This doesn't change the Zipkin Reporter batching but instead augments it to
 * lower API requests to SQS.
 *
 * This sends (usually TBinaryProtocol big-endian) encoded spans to an SQS queue.
 */
@AutoValue
public abstract class AwsBufferedSqsSender extends LazyCloseable<AmazonSQSBufferedAsyncClient> implements Sender {

  public static AwsBufferedSqsSender create(String url) {
    return builder().queueUrl(url).build();
  }

  public static Builder builder() {
    return new AutoValue_AwsBufferedSqsSender.Builder()
        .encoding(Encoding.THRIFT)
        .credentialsProvider(new DefaultAWSCredentialsProviderChain())
        .messageMaxBytes(256 * 1024 - 512); // 256KB SQS limit.  Leave some slack for attributes.
  }

  @AutoValue.Builder
  public interface Builder {

    /** SQS queue URL to send spans. */
    Builder queueUrl(String queueUrl);

    /** AWS credentials for authenticating calls to SQS. */
    Builder credentialsProvider(AWSCredentialsProvider credentialsProvider);

    /** Controls the byte encoding when sending spans. */
    Builder encoding(Encoding encoding);

    /** Maximum size of a message. SQS max message size is 256KB including attributes. */
    Builder messageMaxBytes(int messageMaxBytes);

    AwsBufferedSqsSender build();
  }

  public Builder toBuilder() {
    return new AutoValue_AwsBufferedSqsSender.Builder(this);
  }

  abstract String queueUrl();

  @Nullable abstract AWSCredentialsProvider credentialsProvider();

  private final AtomicBoolean closeCalled = new AtomicBoolean(false);

  @Override public CheckResult check() {
    // TODO need to do something better here.
    return CheckResult.OK;
  }

  @Override protected AmazonSQSBufferedAsyncClient compute() {
    final AmazonSQSAsyncClient client = new AmazonSQSAsyncClient(credentialsProvider());
    return new AmazonSQSBufferedAsyncClient(client);
  }

  @Override public int messageSizeInBytes(final List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  @Override public void sendSpans(final List<byte[]> list, final Callback callback) {
    if (closeCalled.get()) throw new IllegalStateException("closed");

    checkNotNull(list, "list of encoded spans must not be null");

    final byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);

    final SendMessageRequest request = new SendMessageRequest(queueUrl(), "zipkin");

    request.addMessageAttributesEntry(
        "spans",
        new MessageAttributeValue()
            .withDataType("Binary." + encoding().name())
            .withBinaryValue(ByteBuffer.wrap(encodedSpans)));

    get().sendMessageAsync(request, new AsyncHandlerAdapter(callback));
  }

  @Override public void close() throws IOException {
    if (!closeCalled.getAndSet(true)) {
      final AmazonSQSBufferedAsyncClient maybeNull = maybeNull();
      if (maybeNull != null) maybeNull.shutdown();
    }
  }

  private static final class AsyncHandlerAdapter implements
      AsyncHandler<SendMessageRequest, SendMessageResult> {

    private final Callback callback;

    AsyncHandlerAdapter(final Callback callback) {
      this.callback = callback;
    }

    @Override public void onError(final Exception e) {
      callback.onError(e);
    }

    @Override
    public void onSuccess(final SendMessageRequest request,
        final SendMessageResult sendMessageResult) {
      callback.onComplete();
    }
  }

  AwsBufferedSqsSender() {}
}
