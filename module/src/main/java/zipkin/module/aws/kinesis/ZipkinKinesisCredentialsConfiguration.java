/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.kinesis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

@Configuration
@EnableConfigurationProperties(ZipkinKinesisCollectorProperties.class)
@Conditional(ZipkinKinesisCollectorModule.KinesisSetCondition.class)
class ZipkinKinesisCredentialsConfiguration {

  /** Setup {@link StsClient} client an IAM role to assume is given. */
  @Bean
  @ConditionalOnMissingBean
  @Conditional(STSSetCondition.class)
  StsClient securityTokenService(ZipkinKinesisCollectorProperties properties) {
    return StsClient.builder()
        .credentialsProvider(getDefaultCredentialsProvider(properties))
        .region(Region.of(properties.getAwsStsRegion()))
        .build();
  }

  @Autowired(required = false)
  private StsClient securityTokenService;

  /** By default, get credentials from the {@link DefaultCredentialsProvider */
  @Bean
  @ConditionalOnMissingBean
  AwsCredentialsProvider credentialsProvider(ZipkinKinesisCollectorProperties properties) {
    if (securityTokenService != null) {
      return StsAssumeRoleCredentialsProvider.builder()
          .stsClient(securityTokenService)
          .refreshRequest(AssumeRoleRequest.builder()
              .roleArn(properties.getAwsStsRoleArn())
              .roleSessionName("zipkin-server")
              .build())
          .build();
    } else {
      return getDefaultCredentialsProvider(properties);
    }
  }

  private static AwsCredentialsProvider getDefaultCredentialsProvider(
      ZipkinKinesisCollectorProperties properties) {
    AwsCredentialsProvider provider = DefaultCredentialsProvider.create();

    // Create credentials provider from ID and secret if given.
    if (notNullOrEmpty(properties.getAwsAccessKeyId())
        && notNullOrEmpty(properties.getAwsSecretAccessKey())) {
      provider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(properties.getAwsAccessKeyId(), properties.getAwsSecretAccessKey()));
    }

    return provider;
  }

  private static boolean notNullOrEmpty(String value) {
    return (value != null && !value.isEmpty());
  }

  /**
   * This condition passes when {@link ZipkinKinesisCollectorProperties#getAwsStsRoleArn()} is set
   * to non-empty.
   *
   * <p>This is here because the yaml defaults this property to empty like this, and spring-boot
   * doesn't have an option to treat empty properties as unset.
   *
   * <pre>{@code
   * aws-sts-role-arn: ${KINESIS_AWS_STS_ROLE_ARN:}
   * }</pre>
   */
  static final class STSSetCondition extends SpringBootCondition {

    private static final String PROPERTY_NAME = "zipkin.collector.kinesis.aws-sts-role-arn";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata a) {

      String stsRoleArn = context.getEnvironment().getProperty(PROPERTY_NAME);

      return isEmpty(stsRoleArn)
          ? ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set")
          : ConditionOutcome.match();
    }

    private static boolean isEmpty(String s) {
      return s == null || s.isEmpty();
    }
  }
}
