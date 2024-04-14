/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.xray_udp;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.storage.xray_udp.InternalStorageAccess;

import static org.assertj.core.api.Assertions.assertThat;

class XRayUDPReporterTest {

  private static EventLoopGroup workerGroup;
  private static Channel serverChannel;
  private static BlockingQueue<byte[]> receivedPayloads;

  private static XRayUDPReporter reporter;

  @BeforeAll
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

    // TODO(anuraaga): Consider changing return type of create to XRayUdpReporter so it's more
    // obvious the type is Closeable.
    reporter = (XRayUDPReporter) XRayUDPReporter
        .create("localhost:" + ((InetSocketAddress) serverChannel.localAddress()).getPort());
  }

  @AfterAll
  public static void stopServer() {
    reporter.close();
    serverChannel.close().syncUninterruptibly();
    workerGroup.shutdownGracefully();
  }

  @BeforeEach void setUp() {
    receivedPayloads.clear();
  }

  @Test void sendSingleSpan() throws Exception {
    reporter.report(TestObjects.CLIENT_SPAN);
    Span spanWithSdk = TestObjects.CLIENT_SPAN.toBuilder()
                                              .putTag("aws.xray.sdk", "Zipkin Brave")
                                              .build();
    assertThat(receivedPayloads.take())
        .containsExactly(InternalStorageAccess.encode(spanWithSdk));
    assertThat(receivedPayloads).isEmpty();
  }
}
