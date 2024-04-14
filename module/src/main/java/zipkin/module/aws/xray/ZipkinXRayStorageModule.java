/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.xray;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinXRayStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "xray")
@ConditionalOnMissingBean(StorageComponent.class)
class ZipkinXRayStorageModule {

  @Bean
  @ConditionalOnMissingBean
  StorageComponent storage(
      ZipkinXRayStorageProperties properties,
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId) {
    return properties.toBuilder().strictTraceId(strictTraceId).build();
  }
}
