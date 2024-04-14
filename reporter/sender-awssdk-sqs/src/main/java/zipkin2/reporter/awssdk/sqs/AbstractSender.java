/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.awssdk.sqs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;

abstract class AbstractSender extends BytesMessageSender.Base {

  final String queueUrl;
  final int messageMaxBytes;
  volatile boolean closeCalled = false;

  AbstractSender(Encoding encoding, int messageMaxBytes, String queueUrl) {
    super(encoding);
    this.queueUrl = queueUrl;
    this.messageMaxBytes = messageMaxBytes;
  }

  @Override public void send(List<byte[]> list) throws IOException {
    if (closeCalled) throw new ClosedSenderException();

    byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);
    String body =
        encoding() == Encoding.JSON && isAscii(encodedSpans)
            ? new String(encodedSpans, StandardCharsets.UTF_8)
            : Base64.getEncoder().encodeToString(encodedSpans);

    call(SendMessageRequest.builder().messageBody(body).queueUrl(queueUrl).build());
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(List<byte[]> list) {
    int listSize = encoding.listSizeInBytes(list);
    return (listSize + 2) * 4 / 3; // account for base64 encoding
  }

  abstract protected void call(SendMessageRequest request) throws IOException;

  boolean isAscii(byte[] encodedSpans) {
    for (int i = 0; i < encodedSpans.length; i++) {
      if (Byte.toUnsignedInt(encodedSpans[i]) >= 0x80) {
        return false;
      }
    }
    return true;
  }
}
