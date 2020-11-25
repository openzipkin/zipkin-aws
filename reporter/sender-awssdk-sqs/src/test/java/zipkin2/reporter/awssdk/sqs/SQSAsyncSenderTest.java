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
package zipkin2.reporter.awssdk.sqs;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.junit.aws.AmazonSQSRule;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

public class SQSAsyncSenderTest {
  @Rule public AmazonSQSRule sqsRule = new AmazonSQSRule().start();

  SqsClient sqsClient = SqsClient.builder()
      .httpClient(UrlConnectionHttpClient.create())
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create(sqsRule.queueUrl()))
      .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .build();

  SQSSender sender =
      SQSSender.newBuilder()
          .queueUrl(sqsRule.queueUrl())
          .sqsClient(sqsClient)
          .build();

  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(readSpans()).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpans_json_unicode() throws Exception {
    Span unicode = CLIENT_SPAN.toBuilder().putTag("error", "\uD83D\uDCA9").build();
    send(unicode).execute();

    assertThat(readSpans()).containsExactly(unicode);
  }

  @Test
  public void sendsSpans_PROTO3() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(readSpans()).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void outOfBandCancel() throws Exception {
    SQSSender.SQSCall call = (SQSSender.SQSCall) send(CLIENT_SPAN, CLIENT_SPAN);
    assertThat(call.isCanceled()).isFalse(); // sanity check

    CountDownLatch latch = new CountDownLatch(1);
    call.enqueue(
        new Callback<Void>() {
          @Override
          public void onSuccess(Void aVoid) {
            call.cancel();
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            latch.countDown();
          }
        });

    latch.await(5, TimeUnit.SECONDS);
    assertThat(call.isCanceled()).isTrue();
  }

  @Test
  public void checkOk() {
    assertThat(sender.check()).isEqualTo(CheckResult.OK);
  }

  Call<Void> send(Span... spans) {
    SpanBytesEncoder bytesEncoder =
        sender.encoding() == Encoding.JSON ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  List<Span> readSpans() {
    assertThat(sqsRule.queueCount()).isEqualTo(1);
    return sqsRule.getSpans();
  }
}
