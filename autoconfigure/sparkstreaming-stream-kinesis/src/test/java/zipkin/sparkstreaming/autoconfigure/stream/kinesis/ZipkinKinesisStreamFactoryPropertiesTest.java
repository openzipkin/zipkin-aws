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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.EnvironmentTestUtils.addEnvironment;

@RunWith(Parameterized.class)
public class ZipkinKinesisStreamFactoryPropertiesTest {

    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (context != null) context.close();
    }

    @Parameterized.Parameter(0) public String property;
    @Parameterized.Parameter(1) public Object value;
    @Parameterized.Parameter(2) public Function<ZipkinKinesisStreamFactoryProperties, Object> extractor;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                parameters("kinesis-stream", "zapkin", p -> p.getKinesisStream()),
                parameters("app", "zapkin", p -> p.getApp()),
                parameters("region", "us-east-4", p -> p.getRegion()),
                parameters("kinesis-endpoint", "127.0.0.1:3001", p -> p.getKinesisEndpoint()),
                parameters("checkpoint-interval-millis", 9999, p -> p.getCheckpointIntervalMillis()),
                parameters("awsAccessKeyId", "notmykey", p -> p.getAwsAccessKeyId()),
                parameters("awsSecretKey", "notmysecret", p -> p.getAwsSecretKey())
        });
    }

    static <T> Object[] parameters(String propertySuffix, T value,
                                   Function<ZipkinKinesisStreamFactoryProperties, T> extractor) {
        return new Object[] {"zipkin.sparkstreaming.stream.kinesis." + propertySuffix, value, extractor};
    }


    @Test
    public void canOverrideValueOf() {
        addEnvironment(context, property + ":" + value);

        context.register(
                PropertyPlaceholderAutoConfiguration.class,
                EnableKinesisStreamFactoryProperties.class
        );
        context.refresh();

        assertThat(context.getBean(ZipkinKinesisStreamFactoryProperties.class))
                .extracting(extractor)
                .containsExactly(value);
    }

    @Configuration
    @EnableConfigurationProperties(ZipkinKinesisStreamFactoryProperties.class)
    static class EnableKinesisStreamFactoryProperties {
    }

}
