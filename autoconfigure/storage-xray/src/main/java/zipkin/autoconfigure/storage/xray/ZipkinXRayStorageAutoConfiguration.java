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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.internal.V2StorageComponent;
import zipkin.storage.StorageComponent;
import zipkin2.storage.xray_udp.XRayUDPStorage;

@Configuration
@EnableConfigurationProperties(zipkin.autoconfigure.storage.xray.ZipkinXRayStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "xray")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinXRayStorageAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  V2StorageComponent storage(
      zipkin.autoconfigure.storage.xray.ZipkinXRayStorageProperties properties,
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId) {
    XRayUDPStorage result = properties.toBuilder().strictTraceId(strictTraceId).build();
    return V2StorageComponent.create(result);
  }

  @Bean XRayUDPStorage v2Storage(V2StorageComponent component) {
    return (XRayUDPStorage) component.delegate();
  }
}
