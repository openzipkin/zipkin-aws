/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.xray;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.storage.xray_udp.XRayUDPStorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZipkinXRayStorageModuleTest {

  AnnotationConfigApplicationContext context;

  @AfterEach void close() {
    if (context != null) {
      context.close();
    }
  }

  @Test void doesntProvideStorageComponent_whenStorageTypeNotXRay() {
    assertThrows(NoSuchBeanDefinitionException.class, () -> {
      context = new AnnotationConfigApplicationContext();
      TestPropertyValues.of("zipkin.storage.type:elasticsearch").applyTo(context);
      context.register(
          PropertyPlaceholderAutoConfiguration.class, ZipkinXRayStorageModule.class);
      context.refresh();

      context.getBean(XRayUDPStorage.class);
    });
  }

  @Test void providesStorageComponent_whenStorageTypeXRay() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:xray").applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class, ZipkinXRayStorageModule.class);
    context.refresh();

    assertThat(context.getBean(XRayUDPStorage.class)).isNotNull();
  }

  @Test void canOverrideProperty_daemonAddress() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:xray", "zipkin.storage.xray.daemon-address:localhost:3000"
    ).applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class, ZipkinXRayStorageModule.class);
    context.refresh();

    assertThat(context.getBean(ZipkinXRayStorageProperties.class).getDaemonAddress())
        .isEqualTo("localhost:3000");
  }
}
