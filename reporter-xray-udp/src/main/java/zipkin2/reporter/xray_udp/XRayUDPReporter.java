/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void report(Span span) {
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
