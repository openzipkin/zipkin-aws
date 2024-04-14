/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.elasticsearch;

final class AWSCredentials {
  interface Provider {
    AWSCredentials get();
  }

  final String accessKey;
  final String secretKey;
  final String sessionToken; // Nullable

  AWSCredentials(String accessKey, String secretKey, String sessionToken) {
    if (accessKey == null) throw new NullPointerException("accessKey == null");
    if (secretKey == null) throw new NullPointerException("secretKey == null");
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
  }
}
