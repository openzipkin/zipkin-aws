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
package zipkin2.storage.xray_udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import zipkin2.Span;
import zipkin2.TestObjects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class XRayUDPStorageTest {

  private static EventLoopGroup workerGroup;
  private static Channel serverChannel;
  private static BlockingQueue<byte[]> receivedPayloads;

  private static XRayUDPStorage storage;

  @BeforeClass
  public static void startServer() {
    workerGroup = new NioEventLoopGroup();
    receivedPayloads = new LinkedBlockingQueue<>();

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
        .channel(NioDatagramChannel.class)
        .handler(new ChannelInitializer<NioDatagramChannel>() {
          @Override protected void initChannel(NioDatagramChannel channel) {
            channel.pipeline().addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
              @Override protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                receivedPayloads.add(ByteBufUtil.getBytes(msg.content()));
              }
            });
          }
        });

    serverChannel = bootstrap.bind(0).syncUninterruptibly().channel();

    storage = XRayUDPStorage.newBuilder()
        .address("localhost:" + ((InetSocketAddress) serverChannel.localAddress()).getPort())
        .build();
  }

  @AfterClass
  public static void stopServer() {
    storage.close();
    serverChannel.close().syncUninterruptibly();
    workerGroup.shutdownGracefully();
  }

  @Before
  public void setUp() {
    receivedPayloads.clear();
  }

  @Test
  public void sendTrace() throws Exception {
    storage.accept(TestObjects.TRACE).execute();
    for (Span span : TestObjects.TRACE) {
      byte[] received = receivedPayloads.take();
      assertThat(received).containsExactly(UDPMessageEncoder.encode(span));
    }
    assertThat(receivedPayloads).isEmpty();
  }

  @Test
  public void sendSingleSpan() throws Exception {
    storage.accept(Collections.singletonList(TestObjects.CLIENT_SPAN)).execute();
    assertThat(receivedPayloads.take())
        .containsExactly(UDPMessageEncoder.encode(TestObjects.CLIENT_SPAN));
    assertThat(receivedPayloads).isEmpty();
  }

  @Test
  public void sendNoSpans() throws Exception {
    storage.accept(Collections.emptyList()).execute();
    // Give some time for any potential bugs to get sent to the server.
    Thread.sleep(100);
    assertThat(receivedPayloads).isEmpty();
  }

  @Test
  public void sendAfterClose() throws Exception {
    XRayUDPStorage storage = XRayUDPStorage.newBuilder()
        .address("localhost:" + ((InetSocketAddress)serverChannel.localAddress()).getPort())
        .build();
    storage.close();
    assertThatThrownBy(() -> storage.accept(TestObjects.TRACE))
        .isInstanceOf(IllegalStateException.class);
  }
}
