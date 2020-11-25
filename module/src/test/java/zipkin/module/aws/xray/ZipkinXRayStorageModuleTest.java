/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
package zipkin.module.aws.xray;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.storage.xray_udp.XRayUDPStorage;

import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinXRayStorageModuleTest {

  AnnotationConfigApplicationContext context;

  @After
  public void close() {
    if (context != null) {
      context.close();
    }
  }

  @Test(expected = NoSuchBeanDefinitionException.class)
  public void doesntProvideStorageComponent_whenStorageTypeNotXRay() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:elasticsearch").applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class, ZipkinXRayStorageModule.class);
    context.refresh();

    context.getBean(XRayUDPStorage.class);
  }

  @Test
  public void providesStorageComponent_whenStorageTypeXRay() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:xray").applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class, ZipkinXRayStorageModule.class);
    context.refresh();

    assertThat(context.getBean(XRayUDPStorage.class)).isNotNull();
  }

  @Test
  public void canOverrideProperty_daemonAddress() {
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
