/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin.junit.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticmq.rest.sqs.SQSLimits;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.rules.ExternalResource;
import zipkin.Codec;
import zipkin.Span;
import zipkin.SpanDecoder;
import zipkin.internal.Util;

import static java.util.Collections.singletonList;


public class AmazonSQSRule extends ExternalResource {

  private SQSRestServer server;
  private AmazonSQSClient client;
  private String queueUrl;

  public AmazonSQSRule() {}

  public AmazonSQSRule start(int httpPort) {
    if (server == null) {
      server = SQSRestServerBuilder
          .withPort(httpPort)
          .withSQSLimits(SQSLimits.Strict())
          .start();
      server.waitUntilStarted();
    }

    if (client == null) {
      client = new AmazonSQSClient(new BasicAWSCredentials("x", "x"));
      client.setEndpoint(String.format("http://localhost:%d", httpPort));
      queueUrl = client.createQueue("zipkin").getQueueUrl();
    }
    return this;
  }

  public String queueUrl() {
    return queueUrl;
  }

  @Override protected void before() {
    if (client != null && queueUrl != null) {
      client.purgeQueue(new PurgeQueueRequest(queueUrl));
    }
  }

  @Override protected void after() {
    if (server != null) {
      server.stopAndWait();
    }
  }

  public int queueCount() {
    String count = client.getQueueAttributes(queueUrl, singletonList("ApproximateNumberOfMessages"))
        .getAttributes()
        .get("ApproximateNumberOfMessages");

    return Integer.valueOf(count);
  }

  public List<Span> getSpans() {
    return getSpans(false);
  }

  public List<Span> getSpans(boolean delete) {

    Stream<Span> spans = Stream.empty();

    ReceiveMessageResult result = client.receiveMessage(queueUrl);

    while(result != null && result.getMessages().size() > 0) {

      spans = Stream.concat(spans,
          result.getMessages().stream().flatMap(AmazonSQSRule::decodeSpans)
      );

      result = client.receiveMessage(queueUrl);

      if (delete) {
        List<DeleteMessageRequest> deletes = result.getMessages().stream()
            .map(m -> new DeleteMessageRequest(queueUrl, m.getReceiptHandle()))
            .collect(Collectors.toList());
        deletes.forEach(d -> client.deleteMessage(d));
      }
    }

    return spans.collect(Collectors.toList());
  }

  public void sendSpans(List<Span> spans) {
    int count = 0;
    List<Span> bucket = new LinkedList<>();

    for (Span span : spans) {
      bucket.add(span);
      if (count++ > 9) {
        sendSpansInternal(bucket);
        bucket = new LinkedList<>();
        count = 0;
      }
    }

    sendSpansInternal(bucket);
  }

  public void send(String body) {
    client.sendMessage(new SendMessageRequest(queueUrl, body));
  }

  private void sendSpansInternal(List<Span> spans) {
    send(Base64.encodeAsString(Codec.THRIFT.writeSpans(spans)));
  }

  static Stream<? extends Span> decodeSpans(Message m) {
    byte[] bytes = m.getBody().charAt(0) == '['
        ? m.getBody().getBytes(Util.UTF_8)
        : Base64.decode(m.getBody());
    return SpanDecoder.DETECTING_DECODER.readSpans(bytes).stream();
  }
}
