/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.sqs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.CollectorSampler;
import zipkin2.collector.sqs.SQSCollector;
import zipkin2.junit.aws.AmazonSQSExtension;
import zipkin2.storage.InMemoryStorage;
import zipkin2.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZipkinSQSCollectorModuleTest {
  @RegisterExtension AmazonSQSExtension sqs = new AmazonSQSExtension();

  AnnotationConfigApplicationContext context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void doesntProvideCollectorComponent_whenSqsQueueUrlUnset() {
    assertThrows(NoSuchBeanDefinitionException.class, () -> {
      context = new AnnotationConfigApplicationContext();
      context.register(
          PropertyPlaceholderAutoConfiguration.class,
          ZipkinSQSCollectorModule.class,
          ZipkinSQSCredentialsConfiguration.class,
          InMemoryConfiguration.class);
      context.refresh();

      context.getBean(SQSCollector.class);
    });
  }

  @Test void provideCollectorComponent_whenSqsQueueUrlIsSet() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.collector.sqs.queue-url:" + sqs.queueUrl(),
        "zipkin.collector.sqs.wait-time-seconds:1",
        "zipkin.collector.sqs.aws-access-key-id: x",
        "zipkin.collector.sqs.aws-secret-access-key: x")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorModule.class,
        ZipkinSQSCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(AwsCredentialsProvider.class)).isNotNull();
    assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
        .isThrownBy(() -> context.getBean(StsClient.class));
  }

  @Test void provideCollectorComponent_setsZipkinSqsCollectorProperties() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.collector.sqs.queue-url:" + sqs.queueUrl(),
        "zipkin.collector.sqs.wait-time-seconds:1",
        "zipkin.collector.sqs.parallelism:3",
        "zipkin.collector.sqs.aws-access-key-id: x",
        "zipkin.collector.sqs.aws-secret-access-key: x")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorModule.class,
        ZipkinSQSCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    ZipkinSQSCollectorProperties properties = context.getBean(ZipkinSQSCollectorProperties.class);

    assertThat(properties.getQueueUrl()).isEqualTo(sqs.queueUrl());
    assertThat(properties.getWaitTimeSeconds()).isEqualTo(1);
    assertThat(properties.getParallelism()).isEqualTo(3);
  }

  @Test void provideSecurityTokenService_whenAwsStsRoleArnIsSet() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.collector.sqs.queue-url:" + sqs.queueUrl(),
        "zipkin.collector.sqs.wait-time-seconds:1",
        "zipkin.collector.sqs.aws-access-key-id: x",
        "zipkin.collector.sqs.aws-secret-access-key: x",
        "zipkin.collector.sqs.aws-sts-role-arn: test",
        "zipkin.collector.sqs.aws-sts-region: ap-southeast-1")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorModule.class,
        ZipkinSQSCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(ZipkinSQSCollectorProperties.class).getAwsStsRegion())
        .isEqualTo("ap-southeast-1");
    assertThat(context.getBean(StsClient.class)).isNotNull();

    assertThat(context.getBean(AwsCredentialsProvider.class))
        .isInstanceOf(StsAssumeRoleCredentialsProvider.class);
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
