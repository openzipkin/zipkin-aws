/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
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
import zipkin2.collector.kinesis.KinesisCollector;
import zipkin2.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinKinesisCollectorProperties.class)
@Conditional(ZipkinKinesisCollectorModule.KinesisSetCondition.class)
@Import(ZipkinKinesisCredentialsConfiguration.class)
class ZipkinKinesisCollectorModule {

  @Bean
  KinesisCollector kinesisCollector(
      ZipkinKinesisCollectorProperties properties,
      AWSCredentialsProvider credentialsProvider,
      CollectorSampler sampler,
      CollectorMetrics metrics,
      StorageComponent storage) {
    return KinesisCollector.newBuilder()
        .credentialsProvider(credentialsProvider)
        .sampler(sampler)
        .metrics(metrics)
        .storage(storage)
        .streamName(properties.getStreamName())
        .appName(properties.getAppName())
        .regionName(properties.getAwsKinesisRegion())
        .build()
        .start();
  }

  static final class KinesisSetCondition extends SpringBootCondition {

    private static final String PROPERTY_NAME = "zipkin.collector.kinesis.stream-name";

    @Override
    public ConditionOutcome getMatchOutcome(
        ConditionContext context, AnnotatedTypeMetadata annotatedTypeMetadata) {
      String streamName = context.getEnvironment().getProperty(PROPERTY_NAME);

      return isEmpty(streamName)
          ? ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set")
          : ConditionOutcome.match();
    }

    private static boolean isEmpty(String s) {
      return s == null || s.isEmpty();
    }
  }
}
