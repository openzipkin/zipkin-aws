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
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.collector.kinesis.KinesisCollector;
import zipkin.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinKinesisCollectorProperties.class)
@Conditional(ZipkinKinesisCollectorAutoConfiguration.KinesisSetCondition.class)
public class ZipkinKinesisCollectorAutoConfiguration {

    @Bean
    KinesisCollector kinesisCollector(ZipkinKinesisCollectorProperties properties, AWSCredentialsProvider credentialsProvider,
                                      CollectorSampler sampler, CollectorMetrics metrics, StorageComponent storage) {
        return KinesisCollector.builder()
                .credentialsProvider(credentialsProvider)
                .sampler(sampler)
                .metrics(metrics)
                .storage(storage)
                .streamName(properties.getStreamName())
                .appName(properties.getAppName())
                .build()
                .start();
    }

    static final class KinesisSetCondition extends SpringBootCondition {

        private static final String PROPERTY_NAME = "zipkin.collector.kinesis.stream-name";

        @Override
        public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata annotatedTypeMetadata) {
            String streamName = context.getEnvironment().getProperty(PROPERTY_NAME);

            return isEmpty(streamName) ?
                    ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set") :
                    ConditionOutcome.match();
        }

        private static boolean isEmpty(String s) {
            return s == null || s.isEmpty();
        }
    }

}
