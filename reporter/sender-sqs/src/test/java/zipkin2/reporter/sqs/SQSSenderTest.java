/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Span;
import zipkin2.junit.aws.AmazonSQSExtension;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

class SQSSenderTest {
  @RegisterExtension AmazonSQSExtension sqs = new AmazonSQSExtension();

  private SQSSender sender;

  @BeforeEach void setup() {
    sender =
        SQSSender.newBuilder()
            .queueUrl(sqs.queueUrl())
            .endpointConfiguration(new EndpointConfiguration(sqs.queueUrl(), "us-east-1"))
            .credentialsProvider(
                new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
            .build();
  }

  @Test void send() {
    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(readSpans()).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_empty() {
    sendSpans();

    assertThat(readSpans()).isEmpty();
  }

  @Test void send_json_unicode() {
    Span unicode = CLIENT_SPAN.toBuilder().putTag("error", "\uD83D\uDCA9").build();
    sendSpans(unicode);

    assertThat(readSpans()).containsExactly(unicode);
  }

  @Test void send_PROTO3() {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(readSpans()).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  void sendSpans(Span... spans) {
    SpanBytesEncoder bytesEncoder =
        sender.encoding() == Encoding.JSON ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  List<Span> readSpans() {
    assertThat(sqs.queueCount()).isEqualTo(1);
    return sqs.getSpans();
  }
}
