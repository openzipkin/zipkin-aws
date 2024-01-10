/*
 * Copyright 2016-2024 The OpenZipkin Authors
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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

/**
 * A Storage implementation which sends Zipkin traces to AWS X-Ray's daemon via UDP.
 */
public final class XRayUDPStorage extends StorageComponent implements SpanStore, SpanConsumer {

  static final int PACKET_LENGTH = 256 * 1024;
  static final ThreadLocal<byte[]> BUF = ThreadLocal.withInitial(() -> new byte[PACKET_LENGTH]);

  public static Builder newBuilder() {
    return new Builder();
  }

  final InetSocketAddress address;
  /** get and close are typically called from different threads */
  volatile DatagramSocket socket;
  volatile boolean closeCalled;

  XRayUDPStorage(InetSocketAddress address) {
    this.address = address;
  }

  DatagramSocket socket() {
    if (socket == null) {
      synchronized (this) {
        if (socket == null) {
          try {
            socket = new DatagramSocket();
          } catch (SocketException e) {
            throw new IllegalStateException(e);
          }
        }
      }
    }
    return socket;
  }

  @Override
  public SpanStore spanStore() {
    return this;
  }

  @Override
  public SpanConsumer spanConsumer() {
    return this;
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (closeCalled) throw new IllegalStateException("closed");
    if (spans.isEmpty()) return Call.create(null);

    int length = spans.size();
    if (length == 1) { // don't allocate an array for a single span
      return new UDPCall(Collections.singletonList(UDPMessageEncoder.encode(spans.get(0))));
    }

    List<byte[]> encoded = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      encoded.add(UDPMessageEncoder.encode(spans.get(i)));
    }
    return new UDPCall(encoded);
  }

  // Synchronous sending eliminates a risk of lost spans when not closing the reporter.
  // TODO: benchmark to see if the overhead is ok
  void send(byte[] message) throws IOException {
    DatagramPacket packet = new DatagramPacket(BUF.get(), PACKET_LENGTH, address);
    packet.setData(message);
    socket().send(packet);
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    DatagramSocket socket = this.socket;
    if (socket != null) socket.close();
    closeCalled = true;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
    throw new UnsupportedOperationException("This is collector-only at the moment");
  }

  @Override public Call<List<Span>> getTrace(String s) {
    throw new UnsupportedOperationException("This is collector-only at the moment");
  }

  @Override public Call<List<String>> getServiceNames() {
    throw new UnsupportedOperationException("This is collector-only at the moment");
  }

  @Override public Call<List<String>> getSpanNames(String s) {
    throw new UnsupportedOperationException("This is collector-only at the moment");
  }

  @Override public Call<List<DependencyLink>> getDependencies(long l, long l1) {
    throw new UnsupportedOperationException("This is collector-only at the moment");
  }

  public static final class Builder extends StorageComponent.Builder {
    String address;

    /** Ignored as AWS X-Ray doesn't accept 64-bit trace IDs */
    @Override
    public Builder strictTraceId(boolean strictTraceId) {
      return this;
    }

    /** Ignored as AWS X-Ray doesn't expose storage options */
    @Override
    public Builder searchEnabled(boolean searchEnabled) {
      return this;
    }

    /** Defaults to the env variable AWS_XRAY_DAEMON_ADDRESS or localhost:2000 */
    public Builder address(String address) {
      if (address == null) throw new IllegalArgumentException("address == null");
      this.address = address;
      return this;
    }

    @Override public XRayUDPStorage build() {
      String address = this.address;
      if (address == null) {
        address = System.getenv("AWS_XRAY_DAEMON_ADDRESS");
        if (address == null || address.isEmpty()) {
          return new XRayUDPStorage(new InetSocketAddress("localhost", 2000));
        } // otherwise fall through to parse
      }
      String[] splitAddress = address.split(":", 2);
      String host = splitAddress[0];
      Integer port = null;
      try {
        if (splitAddress.length == 2) port = Integer.parseInt(splitAddress[1]);
      } catch (NumberFormatException ignore) {
      }
      return new XRayUDPStorage(new InetSocketAddress(host, port));
    }

    Builder() {
    }
  }

  @Override public String toString() {
    return "XRayUDPStorage{address=" + address + "}";
  }

  class UDPCall extends Call.Base<Void> {
    private final List<byte[]> messages;

    UDPCall(List<byte[]> messages) {
      this.messages = messages;
    }

    @Override
    protected Void doExecute() throws IOException {
      for (byte[] message : messages) send(message);
      return null;
    }

    @Override
    protected void doEnqueue(Callback<Void> callback) {
      try {
        doExecute();
        callback.onSuccess(null);
      } catch (IOException | RuntimeException | Error e) {
        propagateIfFatal(e);
        callback.onError(e);
      }
    }

    @Override
    public Call<Void> clone() {
      return new UDPCall(messages);
    }
  }
}
