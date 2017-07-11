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
package zipkin.autoconfigure.collector.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.collector.sqs.SQSCollector;
import zipkin.junit.aws.AmazonSQSRule;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.StorageComponent;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinSQSCollectorAutoConfigurationTest {
  /** Don't crash if CI box doesn't have .aws directory defined */
  @Configuration static class Region {
    @Bean EndpointConfiguration endpointConfiguration(){
      return new EndpointConfiguration("sqs.us-east-1.amazonaws.com", "us-east-1");
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  AnnotationConfigApplicationContext context;

  @After
  public void close() {
    if (context != null) context.close();
  }

  @Test
  public void doesntProvideCollectorComponent_whenSqsQueueUrlUnset() {
    context = new AnnotationConfigApplicationContext();
    context.register(PropertyPlaceholderAutoConfiguration.class, Region.class,
        ZipkinSQSCollectorAutoConfiguration.class, ZipkinSQSCredentialsAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(SQSCollector.class);
  }

  @Test
  public void provideCollectorComponent_whenSqsQueueUrlIsSet() {
    context = new AnnotationConfigApplicationContext();
    addEnvironment(context, "zipkin.collector.sqs.queue-url:" + sqsRule.queueUrl());
    addEnvironment(context, "zipkin.collector.sqs.wait-time-seconds:1");
    addEnvironment(context, "zipkin.collector.sqs.aws-access-key-id: x");
    addEnvironment(context, "zipkin.collector.sqs.aws-secret-access-key: x");
    context.register(PropertyPlaceholderAutoConfiguration.class, Region.class,
        ZipkinSQSCollectorAutoConfiguration.class, ZipkinSQSCredentialsAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(AWSCredentialsProvider.class)).isNotNull();
    assertThatExceptionOfType(NoSuchBeanDefinitionException.class).isThrownBy(() -> context.getBean(AWSSecurityTokenService.class));
  }

  @Test
  public void provideCollectorComponent_setsZipkinSqsCollectorProperties() {
    context = new AnnotationConfigApplicationContext();
    addEnvironment(context, "zipkin.collector.sqs.queue-url:" + sqsRule.queueUrl());
    addEnvironment(context, "zipkin.collector.sqs.wait-time-seconds:1");
    addEnvironment(context, "zipkin.collector.sqs.parallelism:3");
    addEnvironment(context, "zipkin.collector.sqs.aws-access-key-id: x");
    addEnvironment(context, "zipkin.collector.sqs.aws-secret-access-key: x");
    context.register(PropertyPlaceholderAutoConfiguration.class, Region.class,
        ZipkinSQSCollectorAutoConfiguration.class, ZipkinSQSCredentialsAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    ZipkinSQSCollectorProperties properties = context.getBean(ZipkinSQSCollectorProperties.class);

    assertThat(properties.getQueueUrl()).isEqualTo(sqsRule.queueUrl());
    assertThat(properties.getWaitTimeSeconds()).isEqualTo(1);
    assertThat(properties.getParallelism()).isEqualTo(3);
  }

  @Test
  public void provideSecurityTokenService_whenAwsStsRoleArnIsSet() {
    context = new AnnotationConfigApplicationContext();
    addEnvironment(context, "zipkin.collector.sqs.queue-url:" + sqsRule.queueUrl());
    addEnvironment(context, "zipkin.collector.sqs.wait-time-seconds:1");
    addEnvironment(context, "zipkin.collector.sqs.aws-access-key-id: x");
    addEnvironment(context, "zipkin.collector.sqs.aws-secret-access-key: x");
    addEnvironment(context, "zipkin.collector.sqs.aws-sts-role-arn: test");
    context.register(PropertyPlaceholderAutoConfiguration.class, Region.class,
        ZipkinSQSCollectorAutoConfiguration.class, ZipkinSQSCredentialsAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(AWSSecurityTokenService.class)).isNotNull();
    assertThat(context.getBean(AWSCredentialsProvider.class)).isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
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
      return new InMemoryStorage();
    }
  }
}
