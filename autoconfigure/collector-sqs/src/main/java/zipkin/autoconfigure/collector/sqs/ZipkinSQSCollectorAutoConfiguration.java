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
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.collector.sqs.SQSCollector;
import zipkin.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinSQSCollectorProperties.class)
@Conditional(ZipkinSQSCollectorAutoConfiguration.SQSSetCondition.class)
public class ZipkinSQSCollectorAutoConfiguration {

  /** By default, uses credentials from the {@link DefaultAWSCredentialsProviderChain}. If
   * AWS Access Key ID and AWS Secret Access Key are provided then credentials are created from
   * {@link AWSStaticCredentialsProvider}.  When an STS role ARN is provided then
   * {@link STSAssumeRoleSessionCredentialsProvider} is used as the credentials store.
   * */
  @Bean
  @ConditionalOnMissingBean
  AWSCredentialsProvider credentialsProvider(ZipkinSQSCollectorProperties sqs) {

    // Create credentials provider from ID and secret if given otherwise use the default provider
    AWSCredentialsProvider provider;
    if (!isNullOrEmpty(sqs.awsAccessKeyId) && !isNullOrEmpty(sqs.awsSecretAccessKey)) {
      provider = new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(sqs.awsAccessKeyId, sqs.awsSecretAccessKey));
    } else {
      provider = new DefaultAWSCredentialsProviderChain();
    }

    // Create STS provider if a role is specified
    if (!isNullOrEmpty(sqs.awsStsRoleArn)) {
      AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
          .withCredentials(provider)
          .withRegion(sqs.awsStsRegion)
          .build();
      provider = new STSAssumeRoleSessionCredentialsProvider.Builder(sqs.awsStsRoleArn, "zipkin-server")
          .withStsClient(stsClient)
          .build();
    }

    return provider;
  }

  @Bean SQSCollector sqs(ZipkinSQSCollectorProperties sqs, AWSCredentialsProvider provider,
      CollectorSampler sampler, CollectorMetrics metrics, StorageComponent storage) {
    return sqs.toBuilder()
        .queueUrl(sqs.getQueueUrl())
        .waitTimeSeconds(sqs.getWaitTimeSeconds())
        .parallelism(sqs.getParallelism())
        .credentialsProvider(provider)
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

    @Override public ConditionOutcome getMatchOutcome(ConditionContext context,
        AnnotatedTypeMetadata a) {

      String queueUrl = context.getEnvironment().getProperty(PROPERTY_NAME);

      return isEmpty(queueUrl) ?
          ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set") :
          ConditionOutcome.match();
    }

    private static boolean isEmpty(String s) {
      return s == null || s.isEmpty();
    }
  }

  private static boolean isNullOrEmpty(String value) {
    return (value == null || value.equals(""));
  }

}
