/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.CollectorSampler;
import zipkin2.collector.sqs.SQSCollector;
import zipkin2.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinSQSCollectorProperties.class)
@Conditional(ZipkinSQSCollectorModule.SQSSetCondition.class)
@Import(ZipkinSQSCredentialsConfiguration.class)
@AutoConfigureAfter(name = "zipkin2.server.internal.ZipkinServerConfiguration")
class ZipkinSQSCollectorModule {

  @Autowired(required = false)
  EndpointConfiguration endpointConfiguration;

  @Bean
  SQSCollector sqsCollector(
      ZipkinSQSCollectorProperties properties,
      AWSCredentialsProvider credentialsProvider,
      CollectorSampler sampler,
      CollectorMetrics metrics,
      StorageComponent storage) {
    return properties
        .toBuilder()
        .queueUrl(properties.getQueueUrl())
        .waitTimeSeconds(properties.getWaitTimeSeconds())
        .parallelism(properties.getParallelism())
        .endpointConfiguration(endpointConfiguration)
        .credentialsProvider(credentialsProvider)
        .sampler(sampler)
        .metrics(metrics)
        .storage(storage)
        .build()
        .start();
  }

  /**
   * This condition passes when {@link ZipkinSQSCollectorProperties#getQueueUrl()} is set to
   * non-empty.
   *
   * <p>This is here because the yaml defaults this property to empty like this, and spring-boot
   * doesn't have an option to treat empty properties as unset.
   *
   * <pre>{@code
   * queue-url: ${SQS_QUEUE_URL:}
   * }</pre>
   */
  static final class SQSSetCondition extends SpringBootCondition {

    private static final String PROPERTY_NAME = "zipkin.collector.sqs.queue-url";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata a) {

      String queueUrl = context.getEnvironment().getProperty(PROPERTY_NAME);

      return isEmpty(queueUrl)
          ? ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set")
          : ConditionOutcome.match();
    }

    private static boolean isEmpty(String s) {
      return s == null || s.isEmpty();
    }
  }
}
