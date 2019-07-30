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
package zipkin.autoconfigure.storage.elasticsearch.aws;

import com.linecorp.armeria.client.Client;
import com.linecorp.armeria.client.ClientOptionsBuilder;
import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.client.endpoint.StaticEndpointGroup;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.SessionProtocol;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
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
import static zipkin.autoconfigure.storage.elasticsearch.aws.ZipkinElasticsearchAwsStorageAutoConfiguration.QUALIFIER;

public class ZipkinElasticsearchAwsStorageAutoConfigurationTest {

  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Mock Client<HttpRequest, HttpResponse> mockHttpClient;

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @After public void close() {
    context.close();
  }

  @Test public void providesBeans_whenStorageTypeElasticsearchAndHostsAreAwsUrls() {
    refreshContextWithProperties("zipkin.storage.type:elasticsearch",
        "zipkin.storage.elasticsearch.hosts:https://search-domain-xyzzy.us-west-2.es.amazonaws.com");

    expectSignatureInterceptor();
    expectNoDomainEndpoint();
  }

  @Test public void providesBeans_whenStorageTypeElasticsearchAndDomain() {
    refreshContextWithProperties(
        "zipkin.storage.type:elasticsearch",
        "zipkin.storage.elasticsearch.aws.domain:foobar",
        "zipkin.storage.elasticsearch.aws.region:us-west-2"
    );

    expectSignatureInterceptor();
    expectDomainEndpoint();
  }

  @Test public void doesntProvideBeans_whenStorageTypeNotElasticsearch() {
    refreshContextWithProperties("zipkin.storage.type:cassandra");

    expectNoInterceptors();
    expectNoDomainEndpoint();
  }

  @Test public void doesntProvideBeans_whenStorageTypeElasticsearchAndHostsNotUrls() {
    refreshContextWithProperties("zipkin.storage.type:elasticsearch");

    expectNoInterceptors();
    expectNoDomainEndpoint();
  }

  @Test public void doesntProvideBeans_whenStorageTypeElasticsearchAndHostsNotAwsUrls() {
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

    ClientOptionsBuilder clientBuilder = new ClientOptionsBuilder();

    // TODO(anuraaga): Can be simpler after https://github.com/line/armeria/issues/1883
    customizer.accept(clientBuilder);
    Client<HttpRequest, HttpResponse> decorated = clientBuilder.build().decoration()
        .decorate(HttpRequest.class, HttpResponse.class, mockHttpClient);
    assertThat(decorated).isInstanceOf(AWSSignatureVersion4.class);
  }

  static class TestGraph { // easier than generics with getBean
    @Autowired @Qualifier(QUALIFIER) SessionProtocol esSessionProtocol;
    @Autowired @Qualifier(QUALIFIER) Supplier<EndpointGroup> endpointGroupSupplier;
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
        ZipkinElasticsearchAwsStorageAutoConfiguration.class,
        TestGraph.class);
    context.refresh();
  }

  @Configuration static class DefaultHostsConfiguration {
    @Bean Function<Endpoint, HttpClient> esHttpClientFactory() {
      return (endpoint) -> HttpClient.of(HTTP, endpoint);
    }

    @Bean @Qualifier(QUALIFIER) @ConditionalOnMissingBean SessionProtocol esSessionProtocol() {
      return SessionProtocol.HTTP;
    }

    @Bean @Qualifier(QUALIFIER) @ConditionalOnMissingBean
    Supplier<EndpointGroup> esInitialEndpoints() {
      return StaticEndpointGroup::new;
    }
  }
}
