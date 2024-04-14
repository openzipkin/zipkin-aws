/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;
import java.nio.charset.Charset;
import java.util.List;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.internal.Nullable;

/**
 * Zipkin {@link BytesMessageSender} implementation that sends spans to an SQS queue.
 *
 * <p>The {@link AsyncReporter} batches spans into a single message to improve throughput and lower
 * API requests to SQS. Based on current service capabilities, a message will contain roughly 256KiB
 * of spans.
 *
 * <p>This sends (usually TBinaryProtocol big-endian) encoded spans to an SQS queue.
 */
public final class SQSSender extends BytesMessageSender.Base {
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

  SQSSender(Builder builder) {
    super(builder.encoding);
    this.queueUrl = builder.queueUrl;
    this.credentialsProvider = builder.credentialsProvider;
    this.endpointConfiguration = builder.endpointConfiguration;
    this.messageMaxBytes = builder.messageMaxBytes;
  }

  /** get and close are typically called from different threads */
  volatile AmazonSQS client;
  volatile boolean closeCalled;

  AmazonSQS get() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = AmazonSQSClientBuilder.standard()
              .withCredentials(credentialsProvider)
              .withEndpointConfiguration(endpointConfiguration)
              .build();
        }
      }
    }
    return client;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    int listSize = encoding().listSizeInBytes(encodedSpans);
    return (listSize + 2) * 4 / 3; // account for base64 encoding
  }

  @Override public void send(List<byte[]> list) {
    if (closeCalled) throw new ClosedSenderException();

    byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);
    String body =
        encoding() == Encoding.JSON && isAscii(encodedSpans) ? new String(encodedSpans, UTF_8)
            : Base64.encodeAsString(encodedSpans);

    get().sendMessage(new SendMessageRequest(queueUrl, body));
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    AmazonSQS client = this.client;
    if (client != null) client.shutdown();
    closeCalled = true;
  }

  @Override public String toString() {
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
}
