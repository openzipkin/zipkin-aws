/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.propagation.aws;

// code originally imported from brave.internal.HexCodec
final class HexCodec {

  /** Inspired by {@code okio.Buffer.writeLong} */
  static void writeHexLong(char[] data, int pos, long v) {
    writeHexByte(data, pos + 0, (byte) ((v >>> 56L) & 0xff));
    writeHexByte(data, pos + 2, (byte) ((v >>> 48L) & 0xff));
    writeHexByte(data, pos + 4, (byte) ((v >>> 40L) & 0xff));
    writeHexByte(data, pos + 6, (byte) ((v >>> 32L) & 0xff));
    writeHexByte(data, pos + 8, (byte) ((v >>> 24L) & 0xff));
    writeHexByte(data, pos + 10, (byte) ((v >>> 16L) & 0xff));
    writeHexByte(data, pos + 12, (byte) ((v >>> 8L) & 0xff));
    writeHexByte(data, pos + 14, (byte) (v & 0xff));
  }

  static final char[] HEX_DIGITS =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  static void writeHexByte(char[] data, int pos, byte b) {
    data[pos + 0] = HEX_DIGITS[(b >> 4) & 0xf];
    data[pos + 1] = HEX_DIGITS[b & 0xf];
  }

  HexCodec() {
  }
}
