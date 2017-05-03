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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.collector.kinesis.KinesisCollector;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinKinesisCollectorAutoConfigurationTest {

    AnnotationConfigApplicationContext context;

    @Before
    public void init() {
        context = new AnnotationConfigApplicationContext();
    }

    @After
    public void close() {
        if (context != null) context.close();
    }

    @Test
    public void kinesisCollectorNotCreatedWhenMissingRequiredConfigValue() {
        context.register(PropertyPlaceholderAutoConfiguration.class, ZipkinKinesisCollectorAutoConfiguration.class);
        context.refresh();

        assertThatExceptionOfType(NoSuchBeanDefinitionException.class).isThrownBy(() -> context.getBean(KinesisCollector.class));
    }

    @Test
    public void kinesisCollectorCreatedWhenAllRequiredValuesAreProvided() {
        addEnvironment(context, "zipkin.collector.kinesis.stream-name: zipkin-test");
        addEnvironment(context, "zipkin.collector.kinesis.app-name: zipkin"); // Not required from the user but required by the collector
        context.register(PropertyPlaceholderAutoConfiguration.class,
                ZipkinKinesisCollectorAutoConfiguration.class,
                ZipkinKinesisCredentialsAutoConfiguration.class,
                InMemoryConfiguration.class);
        context.refresh();

        assertThat(context.getBean(KinesisCollector.class)).isNotNull();
        assertThat(context.getBean(ZipkinKinesisCollectorProperties.class)).isNotNull();
    }

    @Test
    public void kinesisCollectorConfiguredForAWSWithGivenCredentials() {
        addEnvironment(context, "zipkin.collector.kinesis.stream-name: zipkin-test");
        addEnvironment(context, "zipkin.collector.kinesis.app-name: zipkin");
        addEnvironment(context, "zipkin.collector.kinesis.aws-access-key-id: x");
        addEnvironment(context, "zipkin.collector.kinesis.aws-secret-access-key: x");
        addEnvironment(context, "zipkin.collector.kinesis.aws-sts-role-arn: test");
        context.register(PropertyPlaceholderAutoConfiguration.class,
                ZipkinKinesisCollectorAutoConfiguration.class,
                ZipkinKinesisCredentialsAutoConfiguration.class,
                InMemoryConfiguration.class);
        context.refresh();

        assertThat(context.getBean(KinesisCollector.class)).isNotNull();
        assertThat(context.getBean(AWSSecurityTokenService.class)).isNotNull();
        assertThat(context.getBean(AWSCredentialsProvider.class)).isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
    }

    @Configuration
    static class InMemoryConfiguration {
        @Bean
        CollectorSampler sampler() {
            return CollectorSampler.ALWAYS_SAMPLE;
        }

        @Bean
        CollectorMetrics metrics() {
            return CollectorMetrics.NOOP_METRICS;
        }

        @Bean
        StorageComponent storage() {
            return new InMemoryStorage();
        }
    }
}
