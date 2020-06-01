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
import java.util.concurrent.TimeUnit;
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

import static brave.Span.Kind.PRODUCER;
import static org.assertj.core.api.Assertions.assertThat;

public class ITSendMessageTracingExecutionInterceptor extends ITRemote {

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
            new SendMessageTracingExecutionInterceptor(tracing)))
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
  public void spanInformationIsSharedOverSqsMessageAttributes() throws Exception {
    // arrange
    final ScopedSpan scopedSpan = tracing.tracer().startScopedSpan("test-span");

    // act
    final String messageId =
        sqsAsyncClient.sendMessage(builder -> builder.queueUrl(queueUrl).messageBody("body"))
            .get(5, TimeUnit.SECONDS)
            .messageId();

    final ReceiveMessageResponse receiveMessageResponse =
        sqsAsyncClient.receiveMessage(builder -> builder
            .queueUrl(queueUrl)
            .waitTimeSeconds(5)
            .messageAttributeNames("b3")
        ).get(5, TimeUnit.SECONDS);
    scopedSpan.finish();

    // assert
    assertThat(receiveMessageResponse.messages()).hasSize(1);
    final Message message = receiveMessageResponse.messages().get(0);
    assertThat(message.messageId()).isEqualTo(messageId);
    final TraceContextOrSamplingFlags traceContextOrSamplingFlags =
        SendMessageRemoteGetter.create(tracing).extract(message.messageAttributes());
    final MutableSpan sendMessageSpan = testSpanHandler.takeRemoteSpan(PRODUCER);
    final MutableSpan testSpan = testSpanHandler.takeLocalSpan();
    assertThat(traceContextOrSamplingFlags.context().traceIdString()).isEqualTo(
        sendMessageSpan.traceId());
    assertThat(traceContextOrSamplingFlags.context().traceIdString()).isEqualTo(testSpan.traceId());
  }
}
