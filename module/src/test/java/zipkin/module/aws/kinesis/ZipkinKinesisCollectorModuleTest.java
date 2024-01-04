/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
package zipkin.module.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.CollectorSampler;
import zipkin2.collector.kinesis.KinesisCollector;
import zipkin2.storage.InMemoryStorage;
import zipkin2.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ZipkinKinesisCollectorModuleTest {

  AnnotationConfigApplicationContext context;

  @BeforeEach void init() {
    context = new AnnotationConfigApplicationContext();
  }

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void kinesisCollectorNotCreatedWhenMissingRequiredConfigValue() {
    context.register(
        PropertyPlaceholderAutoConfiguration.class, ZipkinKinesisCollectorModule.class);
    context.refresh();

    assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
        .isThrownBy(() -> context.getBean(KinesisCollector.class));
  }

  @Test void kinesisCollectorCreatedWhenAllRequiredValuesAreProvided() {
    TestPropertyValues.of(
        "zipkin.collector.kinesis.stream-name: zipkin-test",
        // The yaml file has a default for this
        "zipkin.collector.kinesis.app-name: zipkin")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinKinesisCollectorModule.class,
        ZipkinKinesisCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(KinesisCollector.class)).isNotNull();
    assertThat(context.getBean(ZipkinKinesisCollectorProperties.class)).isNotNull();
  }

  @Test void kinesisCollectorConfiguredForAWSWithGivenCredentials() {
    TestPropertyValues.of(
        "zipkin.collector.kinesis.stream-name: zipkin-test",
        "zipkin.collector.kinesis.app-name: zipkin",
        "zipkin.collector.kinesis.aws-sts-region: us-east-1",
        "zipkin.collector.kinesis.aws-access-key-id: x",
        "zipkin.collector.kinesis.aws-secret-access-key: x",
        "zipkin.collector.kinesis.aws-sts-role-arn: test")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinKinesisCollectorModule.class,
        ZipkinKinesisCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(KinesisCollector.class)).isNotNull();
    assertThat(context.getBean(AWSSecurityTokenService.class)).isNotNull();
    assertThat(context.getBean(AWSCredentialsProvider.class))
        .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
  }

  @Test void kinesisCollectorConfiguredWithCorrectRegion() {
    TestPropertyValues.of(
        "zipkin.collector.kinesis.stream-name: zipkin-test",
        "zipkin.collector.kinesis.app-name: zipkin",
        "zipkin.collector.kinesis.aws-sts-region: us-east-1",
        "zipkin.collector.kinesis.aws-kinesis-region: us-east-1",
        "zipkin.collector.kinesis.aws-access-key-id: x",
        "zipkin.collector.kinesis.aws-secret-access-key: x",
        "zipkin.collector.kinesis.aws-sts-role-arn: test")
        .applyTo(context);

    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinKinesisCollectorModule.class,
        ZipkinKinesisCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    KinesisCollector collector = context.getBean(KinesisCollector.class);

    assertThat(collector)
        .extracting("regionName")
        .as("Kinesis region is set from zipkin.collector.kinesis.aws-kinesis-region")
        .isEqualTo("us-east-1");
  }

  @Configuration
  static class InMemoryConfiguration {
    @Bean
    CollectorSampler sampler() {
      return CollectorSampler.ALWAYS_SAMPLE;
    }

    @Bean
    CollectorMetrics metrics() {
      return CollectorMetrics.NOOP_METRICS;
    }

    @Bean
    StorageComponent storage() {
      return InMemoryStorage.newBuilder().build();
    }
  }
}
