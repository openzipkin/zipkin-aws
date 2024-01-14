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
package zipkin2.reporter.awssdk.sqs;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import zipkin2.Span;
import zipkin2.junit.aws.AmazonSQSExtension;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

class SQSAsyncSenderTest {
  @RegisterExtension AmazonSQSExtension sqs = new AmazonSQSExtension();

  private SQSSender sender;

  @BeforeEach void setup() {
    SqsClient sqsClient = SqsClient.builder()
        .httpClient(UrlConnectionHttpClient.create())
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create(sqs.queueUrl()))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create("x", "x")))
        .build();

    sender = SQSSender.newBuilder()
        .queueUrl(sqs.queueUrl())
        .sqsClient(sqsClient)
        .build();
  }

  @Test void send() throws Exception {
    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(readSpans()).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_empty() throws Exception {
    sendSpans();

    assertThat(readSpans()).isEmpty();
  }

  @Test void send_json_unicode() throws Exception {
    Span unicode = CLIENT_SPAN.toBuilder().putTag("error", "\uD83D\uDCA9").build();
    sendSpans(unicode);

    assertThat(readSpans()).containsExactly(unicode);
  }

  @Test void send_PROTO3() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(readSpans()).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  void sendSpans(Span... spans) throws IOException {
    SpanBytesEncoder bytesEncoder =
        sender.encoding() == Encoding.JSON ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  List<Span> readSpans() {
    assertThat(sqs.queueCount()).isEqualTo(1);
    return sqs.getSpans();
  }
}
