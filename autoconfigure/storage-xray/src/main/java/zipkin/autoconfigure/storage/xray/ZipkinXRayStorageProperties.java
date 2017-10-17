/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin.autoconfigure.storage.xray;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.xray_udp.XRayUDPStorage;

@ConfigurationProperties("zipkin.storage.xray")
public class ZipkinXRayStorageProperties implements Serializable { // for Spark jobs
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
