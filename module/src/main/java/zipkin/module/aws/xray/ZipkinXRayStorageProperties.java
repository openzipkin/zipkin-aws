/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.xray;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.xray_udp.XRayUDPStorage;

@ConfigurationProperties("zipkin.storage.xray")
class ZipkinXRayStorageProperties implements Serializable { // for Spark jobs
  private static final long serialVersionUID = 0L;

  /** Amazon X-Ray Daemon UDP daemon address; defaults to localhost:2000 */
  private String daemonAddress;

  public String getDaemonAddress() {
    return daemonAddress;
  }

  public void setDaemonAddress(String daemonAddress) {
    this.daemonAddress = "".equals(daemonAddress) ? null : daemonAddress;
  }

  public XRayUDPStorage.Builder toBuilder() {
    XRayUDPStorage.Builder builder = XRayUDPStorage.newBuilder();
    if (daemonAddress != null) builder.address(daemonAddress);
    return builder;
  }
}
