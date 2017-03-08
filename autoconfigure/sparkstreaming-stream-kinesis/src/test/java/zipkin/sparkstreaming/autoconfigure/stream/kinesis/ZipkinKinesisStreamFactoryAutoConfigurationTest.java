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
package zipkin.sparkstreaming.autoconfigure.stream.kinesis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin.sparkstreaming.stream.kinesis.KinesisStreamFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinKinesisStreamFactoryAutoConfigurationTest {

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
    public void kinesisStreamNotCreatedWhenMissingRequiredConfigValues() {
        // addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.region: us-east-1");
        context.register(PropertyPlaceholderAutoConfiguration.class, ZipkinKinesisStreamFactoryAutoConfiguration.class);
        context.refresh();

        assertThatExceptionOfType(NoSuchBeanDefinitionException.class).isThrownBy(() -> context.getBean(KinesisStreamFactory.class));
    }

    @Test
    public void kinesisStreamCreatedWhenAllRequiredValuesAreProvided() {
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.region: us-east-1");
        context.register(PropertyPlaceholderAutoConfiguration.class, ZipkinKinesisStreamFactoryAutoConfiguration.class);
        context.refresh();

        assertThat(context.getBean(KinesisStreamFactory.class)).isNotNull();
        assertThat(context.getBean(ZipkinKinesisStreamFactoryProperties.class)).isNotNull();
    }

    @Test
    public void kinesisStreamIsConfigurableWithAllProperties() {
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.kinesis-stream: zapkin");
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.app: zapkin-sparkstreaming");
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.region: us-east-1");
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.kinesis-endpoint: http://localhost:12345");
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.checkpoint-interval-millis: 1234");
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.aws-access-key-id: accesskey");
        addEnvironment(context, "zipkin.sparkstreaming.stream.kinesis.aws-secret-key: itsasecret");
        context.register(PropertyPlaceholderAutoConfiguration.class, ZipkinKinesisStreamFactoryAutoConfiguration.class);
        context.refresh();

        assertThat(context.getBean(KinesisStreamFactory.class)).isNotNull();
        assertThat(context.getBean(ZipkinKinesisStreamFactoryProperties.class)).isNotNull();

        ZipkinKinesisStreamFactoryProperties properties = context.getBean(ZipkinKinesisStreamFactoryProperties.class);

        assertThat(properties.getKinesisStream()).isEqualTo("zapkin");
        assertThat(properties.getApp()).isEqualTo("zapkin-sparkstreaming");
        assertThat(properties.getRegion()).isEqualTo("us-east-1");
        assertThat(properties.getKinesisEndpoint()).isEqualTo("http://localhost:12345");
        assertThat(properties.getCheckpointIntervalMillis()).isEqualTo(1234);
        assertThat(properties.getAwsAccessKeyId()).isEqualTo("accesskey");
        assertThat(properties.getAwsSecretKey()).isEqualTo("itsasecret");
    }
}
