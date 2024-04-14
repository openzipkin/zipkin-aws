/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
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

@Configuration
@EnableConfigurationProperties(ZipkinKinesisCollectorProperties.class)
@Conditional(ZipkinKinesisCollectorModule.KinesisSetCondition.class)
class ZipkinKinesisCredentialsConfiguration {

  /** Setup {@link AWSSecurityTokenService} client an IAM role to assume is given. */
  @Bean
  @ConditionalOnMissingBean
  @Conditional(STSSetCondition.class)
  AWSSecurityTokenService securityTokenService(ZipkinKinesisCollectorProperties properties) {
    return AWSSecurityTokenServiceClientBuilder.standard()
        .withCredentials(getDefaultCredentialsProvider(properties))
        .withRegion(properties.getAwsStsRegion())
        .build();
  }

  @Autowired(required = false)
  private AWSSecurityTokenService securityTokenService;

  /** By default, get credentials from the {@link DefaultAWSCredentialsProviderChain */
  @Bean
  @ConditionalOnMissingBean
  AWSCredentialsProvider credentialsProvider(ZipkinKinesisCollectorProperties properties) {
    if (securityTokenService != null) {
      return new STSAssumeRoleSessionCredentialsProvider.Builder(
              properties.getAwsStsRoleArn(), "zipkin-server")
          .withStsClient(securityTokenService)
          .build();
    } else {
      return getDefaultCredentialsProvider(properties);
    }
  }

  private static AWSCredentialsProvider getDefaultCredentialsProvider(
      ZipkinKinesisCollectorProperties properties) {
    AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();

    // Create credentials provider from ID and secret if given.
    if (notNullOrEmpty(properties.getAwsAccessKeyId())
        && notNullOrEmpty(properties.getAwsSecretAccessKey())) {
      provider =
          new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(properties.getAwsAccessKeyId(), properties.getAwsSecretAccessKey()));
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
