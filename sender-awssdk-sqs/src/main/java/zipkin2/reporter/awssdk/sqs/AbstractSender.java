/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package zipkin2.reporter.awssdk.sqs;

import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import zipkin2.Call;
import zipkin2.codec.Encoding;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Sender;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

abstract class AbstractSender extends Sender {

  final String queueUrl;
  final Encoding encoding;
  final int messageMaxBytes;
  volatile boolean closeCalled = false;

  AbstractSender(Encoding encoding, int messageMaxBytes, String queueUrl) {
    this.queueUrl = queueUrl;
    this.encoding = encoding;
    this.messageMaxBytes = messageMaxBytes;
  }

  @Override public Call<Void> sendSpans(List<byte[]> list) {
    if (closeCalled) throw new IllegalStateException("closed");

    byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);
    String body =
        encoding() == Encoding.JSON && isAscii(encodedSpans)
            ? new String(encodedSpans, StandardCharsets.UTF_8)
            : Base64.getEncoder().encodeToString(encodedSpans);

    return call(SendMessageRequest.builder().messageBody(body).queueUrl(queueUrl).build());
  }

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(List<byte[]> list) {
    return messageSizeInBytes(encoding, list);
  }

  abstract protected Call<Void> call(SendMessageRequest request);

  boolean isAscii(byte[] encodedSpans) {
    for (int i = 0; i < encodedSpans.length; i++) {
      if (encodedSpans[i] >= 0x80) {
        return false;
      }
    }
    return true;
  }

  int messageSizeInBytes(Encoding encoding, List<byte[]> list) {
    int listSize = encoding.listSizeInBytes(list);
    return (listSize + 2) * 4 / 3; // account for base64 encoding
  }

}
