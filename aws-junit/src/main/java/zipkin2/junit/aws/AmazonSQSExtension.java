/*
 * Copyright 2016-2023 The OpenZipkin Authors
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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticmq.StrictSQSLimits$;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

public class AmazonSQSExtension implements BeforeEachCallback, BeforeAllCallback, AfterAllCallback {
  SQSRestServer server;
  int serverPort;
  AmazonSQS client;
  String queueUrl;

  public AmazonSQSExtension() {
  }

  @Override public void beforeAll(ExtensionContext extensionContext) {
    if (server == null) {
      server =
          SQSRestServerBuilder.withDynamicPort().withSQSLimits(StrictSQSLimits$.MODULE$).start();
      serverPort = server.waitUntilStarted().localAddress().getPort();
    }

    if (client == null) {
      client = AmazonSQSClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
          .withEndpointConfiguration(
              new EndpointConfiguration(String.format("http://localhost:%d", serverPort), null))
          .build();
      queueUrl = client.createQueue("zipkin").getQueueUrl();
    }
  }

  @Override public void afterAll(ExtensionContext extensionContext) {
    if (client != null) {
      client.shutdown();
    }

    if (server == null) {
      server.stopAndWait();
    }
  }

  public String queueUrl() {
    return queueUrl;
  }

  @Override public void beforeEach(ExtensionContext extensionContext) {
    if (client != null && queueUrl != null) {
      client.purgeQueue(new PurgeQueueRequest(queueUrl));
    }
  }

  public int queueCount() {
    String count = client.getQueueAttributes(queueUrl, singletonList("ApproximateNumberOfMessages"))
        .getAttributes()
        .get("ApproximateNumberOfMessages");

    return Integer.parseInt(count);
  }

  public int notVisibleCount() {
    String count =
        client.getQueueAttributes(queueUrl, singletonList("ApproximateNumberOfMessagesNotVisible"))
            .getAttributes()
            .get("ApproximateNumberOfMessagesNotVisible");

    return Integer.parseInt(count);
  }

  public List<Span> getSpans() {
    return getSpans(false);
  }

  public List<Span> getSpans(boolean delete) {

    Stream<Span> spans = Stream.empty();

    ReceiveMessageResult result = client.receiveMessage(queueUrl);

    while (result != null && result.getMessages().size() > 0) {

      spans = Stream.concat(spans,
          result.getMessages().stream().flatMap(AmazonSQSExtension::decodeSpans));

      result = client.receiveMessage(queueUrl);

      if (delete) {
        List<DeleteMessageRequest> deletes = result.getMessages()
            .stream()
            .map(m -> new DeleteMessageRequest(queueUrl, m.getReceiptHandle()))
            .collect(Collectors.toList());
        deletes.forEach(d -> client.deleteMessage(d));
      }
    }

    return spans.collect(Collectors.toList());
  }

  public void send(String body) {
    client.sendMessage(new SendMessageRequest(queueUrl, body));
  }

  static Stream<? extends Span> decodeSpans(Message m) {
    byte[] bytes =
        m.getBody().charAt(0) == '[' ? m.getBody().getBytes(UTF_8) : Base64.decode(m.getBody());
    if (bytes[0] == '[') {
      return SpanBytesDecoder.JSON_V2.decodeList(bytes).stream();
    }
    return SpanBytesDecoder.PROTO3.decodeList(bytes).stream();
  }
}
