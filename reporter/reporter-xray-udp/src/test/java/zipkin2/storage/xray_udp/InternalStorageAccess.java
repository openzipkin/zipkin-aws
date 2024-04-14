/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.xray_udp;

import zipkin2.Span;

/**
 * Classpath accessor to expose package-private methods in the storage.
 */
public final class InternalStorageAccess {
  /** Encode a span for XRay, */
  public static byte[] encode(Span span) {
    return UDPMessageEncoder.encode(span);
  }
}
