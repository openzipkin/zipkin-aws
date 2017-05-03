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
package zipkin.autoconfigure.collector.kinesis;

import com.amazonaws.auth.*;
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
@Conditional(ZipkinKinesisCollectorAutoConfiguration.KinesisSetCondition.class)
public class ZipkinKinesisCredentialsAutoConfiguration {

    /** Setup {@link AWSSecurityTokenService} client an IAM role to assume is given. */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(STSSetCondition.class)
    AWSSecurityTokenService securityTokenService(ZipkinKinesisCollectorProperties properties) {
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
    AWSCredentialsProvider credentialsProvider(ZipkinKinesisCollectorProperties properties) {
        if (securityTokenService != null) {
            return new STSAssumeRoleSessionCredentialsProvider.Builder(properties.awsStsRoleArn, "zipkin-server")
                    .withStsClient(securityTokenService)
                    .build();
        } else {
            return getDefaultCredentialsProvider(properties);
        }
    }

    private static AWSCredentialsProvider getDefaultCredentialsProvider(ZipkinKinesisCollectorProperties properties) {
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();

        // Create credentials provider from ID and secret if given.
        if (!isNullOrEmpty(properties.awsAccessKeyId) && !isNullOrEmpty(properties.awsSecretAccessKey)) {
            provider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(properties.awsAccessKeyId, properties.awsSecretAccessKey));
        }

        return provider;
    }

    private static boolean isNullOrEmpty(String value) {
        return (value == null || value.equals(""));
    }


    /**
     * This condition passes when {@link ZipkinKinesisCollectorProperties#getAwsStsRoleArn()} is set to
     * non-empty.
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

        @Override public ConditionOutcome getMatchOutcome(ConditionContext context,
                                                          AnnotatedTypeMetadata a) {

            String stsRoleArn = context.getEnvironment().getProperty(PROPERTY_NAME);

            return isEmpty(stsRoleArn) ?
                    ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set") :
                    ConditionOutcome.match();
        }

        private static boolean isEmpty(String s) {
            return s == null || s.isEmpty();
        }
    }

}
