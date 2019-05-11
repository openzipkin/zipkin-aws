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

import java.util.Collections;
import java.util.List;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import zipkin2.elasticsearch.ElasticsearchStorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ZipkinElasticsearchAwsStorageAutoConfigurationTest {
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
    assertThat(context.getBean(OkHttpClient.class).networkInterceptors())
        .extracting(Interceptor::getClass)
        .contains((Class) AWSSignatureVersion4.class);
  }

  void expectDomainEndpoint() {
    assertThat(context.getBean(ElasticsearchStorage.HostsSupplier.class))
        .isInstanceOf(ElasticsearchDomainEndpoint.class);
  }

  void expectNoInterceptors() {
    assertThat(context.getBean(OkHttpClient.class).networkInterceptors())
        .isEmpty();
  }

  void expectNoDomainEndpoint() {
    try {
      context.getBean(ElasticsearchStorage.HostsSupplier.class);

      failBecauseExceptionWasNotThrown(NoSuchBeanDefinitionException.class);
    } catch (NoSuchBeanDefinitionException expected) {
    }
  }

  void refreshContextWithProperties(String... pairs) {
    TestPropertyValues.of(pairs).applyTo(context);
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinElasticsearchAwsStorageAutoConfiguration.class,
        ZipkinTestAutoConfiguration.class);
    context.refresh();
  }

  // Relevant parts copied from ZipkinElasticsearchStorageAutoConfiguration for use in tests
  static class ZipkinTestAutoConfiguration {
    @Autowired(required = false) List<Interceptor> networkInterceptors = Collections.emptyList();

    @Bean OkHttpClient okHttpClient() {
      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      for (Interceptor interceptor : networkInterceptors) {
        builder.addNetworkInterceptor(interceptor);
      }
      return builder.build();
    }
  }
}
