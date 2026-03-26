/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.junit.aws;

import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticmq.StrictSQSLimits$;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

public class AmazonSQSExtension implements BeforeEachCallback, AfterEachCallback {
  SQSRestServer server;
  int serverPort;
  SqsClient client;
  String queueUrl;

  public AmazonSQSExtension() {
  }

  @Override public void beforeEach(ExtensionContext extensionContext) {
    if (server == null) {
      server =
          SQSRestServerBuilder.withDynamicPort().withSQSLimits(StrictSQSLimits$.MODULE$).start();
      serverPort = server.waitUntilStarted().localAddress().getPort();
    }

    if (client == null) {
      client = SqsClient.builder()
          .httpClient(UrlConnectionHttpClient.create())
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
          .endpointOverride(URI.create("http://localhost:%d".formatted(serverPort)))
          .region(Region.US_EAST_1)
          .build();
      queueUrl = client.createQueue(b -> b.queueName("zipkin")).queueUrl();
    }

    if (client != null && queueUrl != null) {
      client.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
    }
  }

  @Override public void afterEach(ExtensionContext extensionContext) {
    if (client != null) {
      client.close();
      client = null;
    }

    if (server != null) {
      server.stopAndWait();
      server = null;
    }
  }

  public String queueUrl() {
    return queueUrl;
  }

  public int queueCount() {
    String count = client.getQueueAttributes(b -> b.queueUrl(queueUrl)
            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
        .attributes()
        .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);

    return Integer.parseInt(count);
  }

  public int notVisibleCount() {
    String count = client.getQueueAttributes(b -> b.queueUrl(queueUrl)
            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
        .attributes()
        .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE);

    return Integer.parseInt(count);
  }

  public List<Span> getSpans() {
    return getSpans(false);
  }

  public List<Span> getSpans(boolean delete) {

    Stream<Span> spans = Stream.empty();

    ReceiveMessageResponse result = client.receiveMessage(
        ReceiveMessageRequest.builder().queueUrl(queueUrl).build());

    while (result != null && !result.messages().isEmpty()) {

      spans = Stream.concat(spans,
          result.messages().stream().flatMap(AmazonSQSExtension::decodeSpans));

      result = client.receiveMessage(
          ReceiveMessageRequest.builder().queueUrl(queueUrl).build());

      if (delete) {
        List<DeleteMessageRequest> deletes = result.messages()
            .stream()
            .map(m -> DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build())
            .toList();
        deletes.forEach(d -> client.deleteMessage(d));
      }
    }

    return spans.collect(Collectors.toList());
  }

  public void send(String body) {
    client.sendMessage(SendMessageRequest.builder()
        .queueUrl(queueUrl)
        .messageBody(body)
        .build());
  }

  static Stream<? extends Span> decodeSpans(Message m) {
    byte[] bytes =
        m.body().charAt(0) == '[' ? m.body().getBytes(UTF_8) : Base64.getDecoder().decode(m.body());
    if (bytes[0] == '[') {
      return SpanBytesDecoder.JSON_V2.decodeList(bytes).stream();
    }
    return SpanBytesDecoder.PROTO3.decodeList(bytes).stream();
  }
}
