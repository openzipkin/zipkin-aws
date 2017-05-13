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
package zipkin.reporter.kinesis;

import com.amazonaws.client.builder.AwsClientBuilder;
import io.undertow.Undertow;
import io.undertow.io.Receiver;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Before;
import org.junit.Test;
import zipkin.Codec;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.collector.Collector;
import zipkin.reporter.Encoder;
import zipkin.reporter.internal.AwaitableCallback;
import zipkin.storage.Callback;
import zipkin.storage.InMemoryStorage;

import static io.undertow.util.Headers.CONTENT_TYPE;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class KinesisSenderTest {

  private KinesisSender sender;
  private KinesisHandler kinesis;
  private Undertow server;

  @Before
  public void setup() throws IOException {
    kinesis = new KinesisHandler();

    server = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(kinesis).build();
    server.start();

    String endpoint = "http://127.0.0.1:" +
        ((InetSocketAddress) server.getListenerInfo().get(0).getAddress()).getPort();

    sender = KinesisSender.builder()
        .streamName("test")
        .endpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
        .build();
  }

  @Test
  public void sendsSpans() throws Exception {
    send(TestObjects.TRACE);


    List<Span> expected = TestObjects.TRACE;
    List<List<Span>> traces = kinesis.storage.spanStore().getRawTraces();
    List<Span> spans = traces.stream().flatMap(List::stream).collect(toList());

    assertThat(kinesis.requestCount).isEqualTo(1);
    assertThat(spans.size()).isEqualTo(expected.size());
    assertThat(spans).isEqualTo(expected);
  }

  private void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }

  class KinesisHandler implements HttpHandler {

    int requestCount = 0;
    InMemoryStorage storage = new InMemoryStorage();
    Collector collector = Collector.builder(getClass()).storage(storage).build();

    @Override public void handleRequest(HttpServerExchange httpServerExchange) throws Exception {
      requestCount++;
      httpServerExchange.getRequestReceiver().receiveFullBytes((httpServerExchange1, bytes) ->
          collector.acceptSpans(bytes, Codec.THRIFT, Callback.NOOP));
    }
  }
}
