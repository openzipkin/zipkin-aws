/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.CollectorSampler;
import zipkin2.collector.sqs.SQSCollector;
import zipkin2.junit.aws.AmazonSQSExtension;
import zipkin2.storage.InMemoryStorage;
import zipkin2.storage.StorageComponent;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZipkinSQSCollectorModuleTest {
  @RegisterExtension AmazonSQSExtension sqs = new AmazonSQSExtension();

  /** Don't crash if CI box doesn't have .aws directory defined */
  @Configuration
  static class Region {
    @Bean
    EndpointConfiguration endpointConfiguration() {
      return new EndpointConfiguration("sqs.us-east-1.amazonaws.com", "us-east-1");
    }
  }
  AnnotationConfigApplicationContext context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void doesntProvideCollectorComponent_whenSqsQueueUrlUnset() {
    assertThrows(NoSuchBeanDefinitionException.class, () -> {
      context = new AnnotationConfigApplicationContext();
      context.register(
          PropertyPlaceholderAutoConfiguration.class,
          Region.class,
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
        Region.class,
        ZipkinSQSCollectorModule.class,
        ZipkinSQSCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(AWSCredentialsProvider.class)).isNotNull();
    assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
        .isThrownBy(() -> context.getBean(AWSSecurityTokenService.class));
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
        Region.class,
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
        Region.class,
        ZipkinSQSCollectorModule.class,
        ZipkinSQSCredentialsConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(ZipkinSQSCollectorProperties.class).getAwsStsRegion())
        .isEqualTo("ap-southeast-1");
    assertThat(context.getBean(AWSSecurityTokenService.class)).isNotNull();

    assertThat(context.getBean(AWSCredentialsProvider.class))
        .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
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
