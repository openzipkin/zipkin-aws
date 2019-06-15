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
package zipkin2.junit.aws;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticmq.rest.sqs.SQSLimits;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.rules.ExternalResource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

public class AmazonSQSRule extends ExternalResource {

  private SQSRestServer server;
  private SqsClient client;
  private String queueUrl;

  public AmazonSQSRule() {
  }

  public AmazonSQSRule start(int httpPort) {
    if (server == null) {
      server = SQSRestServerBuilder.withPort(httpPort).withSQSLimits(SQSLimits.Strict()).start();
      server.waitUntilStarted();
    }

    if (client == null) {
      client = SqsClient.builder()
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
          .endpointOverride(URI.create(String.format("http://localhost:%d", httpPort))).build();
      queueUrl =
          client.createQueue(CreateQueueRequest.builder().queueName("zipkin").build()).queueUrl();
    }
    return this;
  }

  public String queueUrl() {
    return queueUrl;
  }

  @Override protected void before() {
    if (client != null && queueUrl != null) {
      client.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
    }
  }

  @Override protected void after() {
    if (server != null) server.stopAndWait();
  }

  public int queueCount() {
    return getAttribute("ApproximateNumberOfMessages");
  }

  public int notVisibleCount() {
    return getAttribute("ApproximateNumberOfMessagesNotVisible");
  }

  private int getAttribute(String approximateNumberOfMessagesNotVisible) {
    String count = client
        .getQueueAttributes(GetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributeNamesWithStrings(approximateNumberOfMessagesNotVisible)
            .build())
        .attributesAsStrings()
        .get(approximateNumberOfMessagesNotVisible);

    return Integer.valueOf(count);
  }

  public List<Span> getSpans() {
    return getSpans(false);
  }

  public List<Span> getSpans(boolean delete) {

    Stream<Span> spans = Stream.empty();

    ReceiveMessageResponse result =
        client.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build());

    while (result != null && result.messages().size() > 0) {
      spans = Stream.concat(spans, result.messages().stream().flatMap(AmazonSQSRule::decodeSpans));

      result = client.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build());

      if (delete) {
        result.messages().forEach(m -> {
          client.deleteMessage(DeleteMessageRequest.builder()
              .queueUrl(queueUrl)
              .receiptHandle(m.receiptHandle())
              .build());
        });
      }
    }

    return spans.collect(Collectors.toList());
  }

  public void send(String body) {
    client.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(body).build());
  }

  static Stream<? extends Span> decodeSpans(Message m) {
    byte[] bytes = m.body().charAt(0) == '['
        ? m.body().getBytes(Charset.forName("UTF-8"))
        : Base64.getDecoder().decode(m.body());
    if (bytes[0] == '[') {
      return SpanBytesDecoder.JSON_V2.decodeList(bytes).stream();
    }
    return SpanBytesDecoder.PROTO3.decodeList(bytes).stream();
  }
}
