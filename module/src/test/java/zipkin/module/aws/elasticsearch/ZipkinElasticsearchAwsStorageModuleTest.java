/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
package zipkin.module.aws.elasticsearch;

import com.linecorp.armeria.client.ClientOptions;
import com.linecorp.armeria.client.ClientOptionsBuilder;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.common.SessionProtocol;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.linecorp.armeria.common.SessionProtocol.HTTP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZipkinElasticsearchAwsStorageModuleTest {
  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @AfterEach public void close() {
    context.close();
  }

  @Test void providesBeans_whenStorageTypeElasticsearchAndHostsAreAwsUrls() {
    refreshContextWithProperties("zipkin.storage.type:elasticsearch",
        "zipkin.storage.elasticsearch.hosts:https://search-domain-xyzzy.us-west-2.es.amazonaws.com");

    expectSignatureInterceptor();
    expectNoDomainEndpoint();
  }

  @Test void providesBeans_whenStorageTypeElasticsearchAndDomain() {
    refreshContextWithProperties(
        "zipkin.storage.type:elasticsearch",
        "zipkin.storage.elasticsearch.aws.domain:foobar",
        "zipkin.storage.elasticsearch.aws.region:us-west-2"
    );

    expectSignatureInterceptor();
    expectDomainEndpoint();
  }

  @Test void doesntProvideBeans_whenStorageTypeNotElasticsearch() {
    refreshContextWithProperties("zipkin.storage.type:cassandra");

    expectNoInterceptors();
    expectNoDomainEndpoint();
  }

  @Test void doesntProvideBeans_whenStorageTypeElasticsearchAndHostsNotUrls() {
    refreshContextWithProperties("zipkin.storage.type:elasticsearch");

    expectNoInterceptors();
    expectNoDomainEndpoint();
  }

  @Test void doesntProvideBeans_whenStorageTypeElasticsearchAndHostsNotAwsUrls() {
    refreshContextWithProperties(
        "zipkin.storage.type:elasticsearch",
        "zipkin.storage.elasticsearch.hosts:https://localhost:9200"
    );

    expectNoInterceptors();
    expectNoDomainEndpoint();
  }

  void expectSignatureInterceptor() {
    @SuppressWarnings("unchecked")
    Consumer<ClientOptionsBuilder> customizer =
        (Consumer<ClientOptionsBuilder>) context.getBean("awsSignatureVersion4", Consumer.class);

    ClientOptionsBuilder clientBuilder = ClientOptions.builder();
    customizer.accept(clientBuilder);

    WebClient client = WebClient.builder("http://127.0.0.1:1234")
        .options(clientBuilder.build())
        .build();

    assertThat(client.as(AWSSignatureVersion4.class)).isNotNull();
  }

  static class TestGraph { // easier than generics with getBean
    @Autowired @Qualifier(ZipkinElasticsearchAwsStorageModule.QUALIFIER) SessionProtocol esSessionProtocol;
    @Autowired @Qualifier(ZipkinElasticsearchAwsStorageModule.QUALIFIER) Supplier<EndpointGroup> endpointGroupSupplier;
  }

  void expectDomainEndpoint() {
    assertThat(context.getBean(TestGraph.class).endpointGroupSupplier)
        .isInstanceOf(ElasticsearchDomainEndpoint.class);
    assertThat(context.getBean(TestGraph.class).esSessionProtocol)
        .isEqualTo(SessionProtocol.HTTPS);
  }

  void expectNoInterceptors() {
    assertThatThrownBy(() -> context.getBean("awsSignatureVersion4", Consumer.class))
        .isInstanceOf(NoSuchBeanDefinitionException.class);
  }

  void expectNoDomainEndpoint() {
    assertThat(context.getBean(TestGraph.class).endpointGroupSupplier)
        .isNotInstanceOf(ElasticsearchDomainEndpoint.class);
    assertThat(context.getBean(TestGraph.class).esSessionProtocol)
        .isEqualTo(SessionProtocol.HTTP);
  }

  void refreshContextWithProperties(String... pairs) {
    TestPropertyValues.of(pairs).applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        DefaultHostsConfiguration.class,
        ZipkinElasticsearchAwsStorageModule.class,
        TestGraph.class);
    context.refresh();
  }

  @Configuration static class DefaultHostsConfiguration {
    @Bean Function<EndpointGroup, WebClient> esHttpClientFactory() {
      return (endpoint) -> WebClient.of(HTTP, endpoint);
    }

    @Bean @Qualifier(ZipkinElasticsearchAwsStorageModule.QUALIFIER) @ConditionalOnMissingBean SessionProtocol esSessionProtocol() {
      return SessionProtocol.HTTP;
    }

    @Bean @Qualifier(ZipkinElasticsearchAwsStorageModule.QUALIFIER) @ConditionalOnMissingBean
    Supplier<EndpointGroup> esInitialEndpoints() {
      return EndpointGroup::of;
    }
  }
}
