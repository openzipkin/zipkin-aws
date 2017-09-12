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
package zipkin.reporter.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Component;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.V2SpanConverter;
import zipkin.junit.aws.AmazonSQSRule;
import zipkin.reporter.Encoder;
import zipkin.reporter.Encoding;
import zipkin.reporter.internal.AwaitableCallback;
import zipkin2.codec.SpanBytesEncoder;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class SQSSenderTest {

  @Rule
  public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  List<Span> spans = asList( // No unicode or data that doesn't translate between json formats
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[0]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[1]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[2])
  );

  SQSSender sender = SQSSender.builder()
      .queueUrl(sqsRule.queueUrl())
      .endpointConfiguration(new EndpointConfiguration(sqsRule.queueUrl(), "us-east-1"))
      .credentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
      .build();

  @Test
  public void sendsSpans_thrift() throws Exception {
    sendSpans(Encoder.THRIFT, spans);
  }

  @Test
  public void sendsSpans_json() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.JSON).build();
    sendSpans(Encoder.JSON, spans);
  }

  @Test
  public void sendsSpans_json_unicode() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.JSON).build();
    sendSpans(Encoder.JSON, TestObjects.TRACE);
  }

  @Test
  public void sendsSpans_json2() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.JSON).build();

    // TODO: make SQS rule work on v2 spans
    sendSpans(new Encoder<Span>() {
      @Override public Encoding encoding() {
        return Encoding.JSON;
      }

      @Override public byte[] encode(Span span) {
        return SpanBytesEncoder.JSON_V2.encode(V2SpanConverter.fromSpan(span).get(0));
      }
    }, spans);
  }

  void sendSpans(Encoder<Span> encoder, List<Span> spans) {
    send(encoder, spans);

    assertThat(sqsRule.queueCount()).isEqualTo(1);

    List<Span> traces = sqsRule.getSpans();
    List<Span> expected = spans;

    assertThat(traces.size()).isEqualTo(expected.size());
    assertThat(traces).isEqualTo(expected);
  }

  @Test
  public void checkOk() throws Exception {
    assertThat(sender.check()).isEqualTo(Component.CheckResult.OK);
  }

  <S> void send(Encoder<S> encoder, List<S> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(encoder::encode).collect(toList()), callback);
    callback.await();
  }
}
