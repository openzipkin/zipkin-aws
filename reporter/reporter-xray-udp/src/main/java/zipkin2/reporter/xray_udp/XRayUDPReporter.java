/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.xray_udp;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.reporter.Reporter;
import zipkin2.storage.xray_udp.XRayUDPStorage;

/**
 * Reports Zipkin spans to AWS X-Ray via the X-Ray daemon, contacted using UDP.
 * <p>
 * Note that, unlike AsyncReporter, this reporter attempts to encode and send
 * the span immediately on the calling thread. As UDP is used, there is no
 * latency in waiting for the daemon to accept and respond to the data.
 */
public class XRayUDPReporter implements Reporter<Span>, Closeable {
  static final Logger logger = Logger.getLogger(XRayUDPReporter.class.getName());

  /**
   * Creates a reporter defaulting to the env variable "AWS_XRAY_DAEMON_ADDRESS" or "localhost:2000"
   */
  public static Reporter<Span> create() {
    return new XRayUDPReporter(XRayUDPStorage.newBuilder().build());
  }

  public static Reporter<Span> create(String address) {
    return new XRayUDPReporter(XRayUDPStorage.newBuilder().address(address).build());
  }

  final XRayUDPStorage delegate;

  XRayUDPReporter(XRayUDPStorage delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void report(Span span) {
    if (!span.tags().containsKey("aws.xray.sdk")) {
      span = span.toBuilder().putTag("aws.xray.sdk", "Zipkin Brave").build();
    }
    Call<Void> call;
    try {
      call = delegate.accept(Collections.singletonList(span));
    } catch (RuntimeException e) {
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "couldn't convert span " + span + " into a UDP message", e);
      }
      return;
    }
    try {
      call.execute();
    } catch (IOException | RuntimeException e) {
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "couldn't send UDP message", e);
      }
    }
  }

  @Override
  public String toString() {
    return "XRayUDPReporter(" + delegate + ")";
  }
}
