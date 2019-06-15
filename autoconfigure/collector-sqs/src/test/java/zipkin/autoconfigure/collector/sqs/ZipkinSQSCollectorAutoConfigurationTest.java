/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package zipkin.autoconfigure.collector.sqs;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import zipkin2.junit.aws.AmazonSQSRule;
import zipkin2.storage.InMemoryStorage;
import zipkin2.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ZipkinSQSCollectorAutoConfigurationTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  AnnotationConfigApplicationContext context= new AnnotationConfigApplicationContext();

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void doesntProvideCollectorComponent_whenSqsQueueUrlUnset() {
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class,
        ZipkinSQSCredentialsAutoConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(SQSCollector.class);
  }

  @Test  public void provideCollectorComponent_whenSqsQueueUrlIsSet() {
    TestPropertyValues.of(
        "zipkin.collector.sqs.queue-url:" + sqsRule.queueUrl(),
        "zipkin.collector.sqs.wait-time-seconds:1",
        "zipkin.collector.sqs.aws-access-key-id: x",
        "zipkin.collector.sqs.aws-secret-access-key: x")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class,
        ZipkinSQSCredentialsAutoConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(AwsCredentialsProvider.class)).isNotNull();
    assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
        .isThrownBy(() -> context.getBean(StsClient.class));
  }

  @Test public void provideCollectorComponent_setsZipkinSqsCollectorProperties() {
    TestPropertyValues.of(
        "zipkin.collector.sqs.queue-url:" + sqsRule.queueUrl(),
        "zipkin.collector.sqs.wait-time-seconds:1",
        "zipkin.collector.sqs.parallelism:3",
        "zipkin.collector.sqs.aws-access-key-id: x",
        "zipkin.collector.sqs.aws-secret-access-key: x")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class,
        ZipkinSQSCredentialsAutoConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    ZipkinSQSCollectorProperties properties = context.getBean(ZipkinSQSCollectorProperties.class);

    assertThat(properties.getQueueUrl()).isEqualTo(sqsRule.queueUrl());
    assertThat(properties.getWaitTimeSeconds()).isEqualTo(1);
    assertThat(properties.getParallelism()).isEqualTo(3);
  }

  @Test public void provideSecurityTokenService_whenAwsStsRoleArnIsSet() {
    TestPropertyValues.of(
        "zipkin.collector.sqs.queue-url:" + sqsRule.queueUrl(),
        "zipkin.collector.sqs.wait-time-seconds:1",
        "zipkin.collector.sqs.aws-access-key-id: x",
        "zipkin.collector.sqs.aws-secret-access-key: x",
        "zipkin.collector.sqs.aws-sts-role-arn: test")
        .applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class,
        ZipkinSQSCredentialsAutoConfiguration.class,
        InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(StsClient.class)).isNotNull();
    assertThat(context.getBean(AwsCredentialsProvider.class))
        .isInstanceOf(StsAssumeRoleCredentialsProvider.class);
  }

  @Configuration
  static class InMemoryConfiguration {
    @Bean CollectorSampler sampler() {
      return CollectorSampler.ALWAYS_SAMPLE;
    }

    @Bean CollectorMetrics metrics() {
      return CollectorMetrics.NOOP_METRICS;
    }

    @Bean StorageComponent storage() {
      return InMemoryStorage.newBuilder().build();
    }
  }
}
