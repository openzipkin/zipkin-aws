/**
 * Copyright 2016 The OpenZipkin Authors
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
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.collector.sqs.SQSCollector;
import zipkin.junit.aws.AmazonSQSRule;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinSQSCollectorAutoConfigurationTest {

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
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(SQSCollector.class);
  }

  @Test
  public void provideCollectorComponent_whenSqsQueueUrlIsSet() {
    context = new AnnotationConfigApplicationContext();
    addEnvironment(context, "zipkin.collector.sqs.queueUrl:" + sqsRule.queueUrl());
    addEnvironment(context, "zipkin.collector.sqs.waitTimeSeconds:1");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    assertThat(context.getBean(SQSCollector.class)).isNotNull();
    assertThat(context.getBean(AWSCredentialsProvider.class)).isNotNull();
  }

  @Test
  public void provideCollectorComponent_setsZipkinSqsCollectorProperties() {
    context = new AnnotationConfigApplicationContext();
    addEnvironment(context, "zipkin.collector.sqs.queueUrl:" + sqsRule.queueUrl());
    addEnvironment(context, "zipkin.collector.sqs.waitTimeSeconds:1");
    addEnvironment(context, "zipkin.collector.sqs.parallelism:3");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinSQSCollectorAutoConfiguration.class, InMemoryConfiguration.class);
    context.refresh();

    ZipkinSQSCollectorProperties properties = context.getBean(ZipkinSQSCollectorProperties.class);


    assertThat(properties.getQueueUrl()).isEqualTo(sqsRule.queueUrl());
    assertThat(properties.getWaitTimeSeconds()).isEqualTo(1);
    assertThat(properties.getParallelism()).isEqualTo(3);
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

    @Bean AWSCredentialsProvider credentialsProvider() { return new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")); }
  }
}
