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
@EnableConfigurationProperties(ZipkinSQSCollectorProperties.class)
@Conditional(ZipkinSQSCollectorAutoConfiguration.SQSSetCondition.class)
class ZipkinSQSCredentialsAutoConfiguration {

  /** Setup {@link StsClient} client an IAM role to assume is given. */
  @Bean
  @ConditionalOnMissingBean
  @Conditional(StsSetCondition.class)
  StsClient stsClient(ZipkinSQSCollectorProperties properties) {
    return StsClient.builder()
        .credentialsProvider(getDefaultCredentialsProvider(properties))
        .region(Region.of(properties.awsStsRegion)).build();
  }

  @Autowired(required = false)
  private StsClient stsClient;

  /** By default, get credentials from the {@link #getDefaultCredentialsProvider */
  @Bean
  @ConditionalOnMissingBean
  AwsCredentialsProvider credentialsProvider(ZipkinSQSCollectorProperties properties) {
    if (stsClient != null) {
      return StsAssumeRoleCredentialsProvider.builder()
          .refreshRequest(AssumeRoleRequest.builder()
              .roleArn(properties.awsStsRoleArn).roleSessionName("zipkin-server").build())
          .stsClient(stsClient)
          .build();
    } else {
      return getDefaultCredentialsProvider(properties);
    }
  }

  private static AwsCredentialsProvider getDefaultCredentialsProvider(
      ZipkinSQSCollectorProperties properties) {
    AwsCredentialsProvider provider = DefaultCredentialsProvider.create();

    // Create credentials provider from ID and secret if given.
    if (!isNullOrEmpty(properties.awsAccessKeyId)
        && !isNullOrEmpty(properties.awsSecretAccessKey)) {
      provider = StaticCredentialsProvider.create(
          AwsBasicCredentials.create(properties.awsAccessKeyId, properties.awsSecretAccessKey));
    }

    return provider;
  }

  private static boolean isNullOrEmpty(String value) {
    return (value == null || value.equals(""));
  }

  /**
   * This condition passes when {@link ZipkinSQSCollectorProperties#getAwsStsRoleArn()} is set to
   * non-empty.
   *
   * <p>This is here because the yaml defaults this property to empty like this, and spring-boot
   * doesn't have an option to treat empty properties as unset.
   *
   * <pre>{@code
   * aws-sts-role-arn: ${SQS_Aws_Sts_ROLE_ARN:}
   * }</pre>
   */
  static final class StsSetCondition extends SpringBootCondition {

    private static final String PROPERTY_NAME = "zipkin.collector.sqs.aws-sts-role-arn";

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
