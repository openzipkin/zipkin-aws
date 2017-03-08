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

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin.sparkstreaming.StreamFactory;

@Configuration
@EnableConfigurationProperties(ZipkinKinesisStreamFactoryProperties.class)
@Conditional(ZipkinKinesisStreamFactoryAutoConfiguration.KinesisStreamConfigurationCondition.class)
public class ZipkinKinesisStreamFactoryAutoConfiguration {

    @Bean
    StreamFactory kinesisStream(ZipkinKinesisStreamFactoryProperties properties) {
        return properties.toBuilder().build();
    }

    static final class KinesisStreamConfigurationCondition extends SpringBootCondition {
        static final String REGION = ZipkinKinesisStreamFactoryProperties.PROPERTIES_ROOT + ".region";
        @Override
        public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata a) {
            String region = context.getEnvironment().getProperty(REGION);

            return region != null ?
                    ConditionOutcome.match() :
                    ConditionOutcome.noMatch("region for kinesis stream not set");
        }
    }

}
