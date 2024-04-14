/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.elasticsearch;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.storage.elasticsearch.aws")
class ZipkinElasticsearchAwsStorageProperties implements Serializable { // for Spark jobs
  private static final long serialVersionUID = 0L;

  /** The name of a domain to look up by endpoint. Exclusive with hosts list. */
  private String domain;

  /**
   * The optional region to search for the domain {@link #domain}. Defaults the usual way
   * (AWS_REGION, DEFAULT_AWS_REGION, etc.).
   */
  private String region;

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = emptyToNull(domain);
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = emptyToNull(region);
  }

  static String emptyToNull(String s) {
    return "".equals(s) ? null : s;
  }
}
