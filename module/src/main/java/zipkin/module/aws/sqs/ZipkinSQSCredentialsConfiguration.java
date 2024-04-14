/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.sqs;

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
@EnableConfigurationProperties(ZipkinSQSCollectorProperties.class)
@Conditional(ZipkinSQSCollectorModule.SQSSetCondition.class)
class ZipkinSQSCredentialsConfiguration {

  /** Setup {@link AWSSecurityTokenService} client an IAM role to assume is given. */
  @Bean
  @ConditionalOnMissingBean
  @Conditional(STSSetCondition.class)
  AWSSecurityTokenService securityTokenService(ZipkinSQSCollectorProperties properties) {
    return AWSSecurityTokenServiceClientBuilder.standard()
        .withCredentials(getDefaultCredentialsProvider(properties))
        .withRegion(properties.awsStsRegion)
        .build();
  }

  @Autowired(required = false)
  private AWSSecurityTokenService securityTokenService;

  /** By default, get credentials from the {@link DefaultAWSCredentialsProviderChain */
  @Bean
  @ConditionalOnMissingBean
  AWSCredentialsProvider credentialsProvider(ZipkinSQSCollectorProperties properties) {
    if (securityTokenService != null) {
      return new STSAssumeRoleSessionCredentialsProvider.Builder(
              properties.awsStsRoleArn, "zipkin-server")
          .withStsClient(securityTokenService)
          .build();
    } else {
      return getDefaultCredentialsProvider(properties);
    }
  }

  private static AWSCredentialsProvider getDefaultCredentialsProvider(
      ZipkinSQSCollectorProperties properties) {
    AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();

    // Create credentials provider from ID and secret if given.
    if (notNullOrEmpty(properties.awsAccessKeyId)
        && notNullOrEmpty(properties.awsSecretAccessKey)) {
      provider =
          new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(properties.awsAccessKeyId, properties.awsSecretAccessKey));
    }

    return provider;
  }

  private static boolean notNullOrEmpty(String value) {
    return (value != null && !value.isEmpty());
  }

  /**
   * This condition passes when {@link ZipkinSQSCollectorProperties#getAwsStsRoleArn()} is set to
   * non-empty.
   *
   * <p>This is here because the yaml defaults this property to empty like this, and spring-boot
   * doesn't have an option to treat empty properties as unset.
   *
   * <pre>{@code
   * aws-sts-role-arn: ${SQS_AWS_STS_ROLE_ARN:}
   * }</pre>
   */
  static final class STSSetCondition extends SpringBootCondition {

    private static final String PROPERTY_NAME = "zipkin.collector.sqs.aws-sts-role-arn";

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
