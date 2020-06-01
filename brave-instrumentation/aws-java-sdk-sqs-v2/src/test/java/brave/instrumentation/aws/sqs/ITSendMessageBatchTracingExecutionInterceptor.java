/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
package brave.instrumentation.aws.sqs;

import akka.http.scaladsl.Http;
import brave.ScopedSpan;
import brave.handler.MutableSpan;
import brave.instrumentation.aws.sqs.propogation.SendMessageRemoteGetter;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.test.ITRemote;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import static brave.Span.Kind.PRODUCER;
import static org.assertj.core.api.Assertions.assertThat;

public class ITSendMessageBatchTracingExecutionInterceptor extends ITRemote {

  private SqsAsyncClient sqsAsyncClient;

  private SQSRestServer sqsRestServer;

  private String queueUrl;

  @Before
  public void setUp() throws Exception {
    sqsRestServer = SQSRestServerBuilder
        .withInterface("localhost")
        .withDynamicPort()
        .start();

    final Http.ServerBinding serverBinding = sqsRestServer.waitUntilStarted();
    final String queueServerUrl = "http://localhost:" + serverBinding.localAddress().getPort();

    sqsAsyncClient = SqsAsyncClient.builder()
        .endpointOverride(URI.create(queueServerUrl))
        .region(Region.of("elastic-mq"))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("accessKeyId", "secretAccessKey")))
        .overrideConfiguration(builder -> builder.addExecutionInterceptor(
            new SendMessageBatchTracingExecutionInterceptor(tracing)))
        .build();

    queueUrl = sqsAsyncClient.createQueue(builder -> builder.queueName("name"))
        .get(5, TimeUnit.SECONDS)
        .queueUrl();
  }

  @After
  public void tearDown() {
    if (sqsRestServer != null) {
      sqsRestServer.stopAndWait();
    }
  }

  @Test
  public void spanInformationIsSharedOverSqsMessageAttributes()
      throws InterruptedException, TimeoutException, ExecutionException {
    // arrange
    final ScopedSpan scopedSpan = tracing.tracer().startScopedSpan("test-span");
    final List<SendMessageBatchRequestEntry> messages = new ArrayList<>();
    messages.add(SendMessageBatchRequestEntry.builder()
        .id("first")
        .messageBody("body-first")
        .build());
    messages.add(SendMessageBatchRequestEntry.builder()
        .id("second")
        .messageBody("body-second")
        .build());

    // act
    sqsAsyncClient.sendMessageBatch(builder -> builder.queueUrl(queueUrl).entries(messages))
        .get(5, TimeUnit.SECONDS);

    final ReceiveMessageResponse receiveMessageResponse =
        sqsAsyncClient.receiveMessage(builder -> builder
            .queueUrl(queueUrl)
            .maxNumberOfMessages(2)
            .waitTimeSeconds(5)
            .messageAttributeNames("b3")
        ).get(5, TimeUnit.SECONDS);
    scopedSpan.finish();

    // assert
    final MutableSpan firstMessageSpan = testSpanHandler.takeRemoteSpan(PRODUCER);
    final MutableSpan secondMessageSpan = testSpanHandler.takeRemoteSpan(PRODUCER);
    final MutableSpan testSpan = testSpanHandler.takeLocalSpan();

    final Message firstMessage =
        getMessage(receiveMessageResponse, firstMessageSpan.tag("message.id"));
    final TraceContextOrSamplingFlags firstMessageContext =
        SendMessageRemoteGetter.create(tracing).extract(firstMessage.messageAttributes());
    assertThat(firstMessageContext.context().traceIdString()).isEqualTo(testSpan.traceId());
    assertThat(firstMessageContext.context().traceIdString()).isEqualTo(firstMessageSpan.traceId());
    assertThat(firstMessageContext.context().spanIdString()).isEqualTo(firstMessageSpan.id());

    final Message secondMessage =
        getMessage(receiveMessageResponse, secondMessageSpan.tag("message.id"));
    final TraceContextOrSamplingFlags secondMessageContext =
        SendMessageRemoteGetter.create(tracing).extract(secondMessage.messageAttributes());
    assertThat(secondMessageContext.context().traceIdString()).isEqualTo(testSpan.traceId());
    assertThat(secondMessageContext.context().traceIdString()).isEqualTo(
        secondMessageSpan.traceId());
    assertThat(secondMessageContext.context().spanIdString()).isEqualTo(secondMessageSpan.id());
  }

  private Message getMessage(final ReceiveMessageResponse response, final String messageId) {
    return response.messages().stream()
        .filter(message -> message.messageId().equals(messageId))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Could not find message with ID: " + messageId));
  }
}
