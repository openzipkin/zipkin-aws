/*
 * Copyright 2016-2018 The OpenZipkin Authors
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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin2.autoconfigure.storage.elasticsearch.ZipkinElasticsearchStorageAutoConfiguration;
import zipkin2.elasticsearch.ElasticsearchStorage;
import zipkin2.storage.StorageComponent;

import static java.lang.String.format;

@Configuration
@EnableConfigurationProperties(ZipkinElasticsearchAwsStorageProperties.class)
@Conditional(ZipkinElasticsearchAwsStorageAutoConfiguration.AwsMagic.class)
@Import(ZipkinElasticsearchStorageAutoConfiguration.class)
class ZipkinElasticsearchAwsStorageAutoConfiguration {
  static final Pattern AWS_URL =
      Pattern.compile("^https://[^.]+\\.([^.]+)\\.es\\.amazonaws\\.com", Pattern.CASE_INSENSITIVE);
  static final Logger log =
      Logger.getLogger(ZipkinElasticsearchAwsStorageAutoConfiguration.class.getName());

  @Bean
  @Qualifier("zipkinElasticsearchHttp")
  Interceptor awsSignatureVersion4(
      String region,
      ZipkinElasticsearchAwsStorageProperties aws,
      AWSCredentials.Provider credentials) {
    return new AWSSignatureVersion4(region, "es", credentials);
  }

  @Bean
  String region(
      @Value("${zipkin.storage.elasticsearch.hosts:}") String hosts,
      ZipkinElasticsearchAwsStorageProperties aws) {
    if ("".equals(hosts)) hosts = null;
    String domain = aws.getDomain();
    if (hosts != null && domain != null) {
      log.warning(
          format(
              "Expected exactly one of hosts or domain: instead saw hosts '%s' and domain '%s'."
                  + " Ignoring hosts and proceeding to look for domain. Either unset ES_HOSTS or "
                  + "ES_AWS_DOMAIN to suppress this message.",
              hosts, domain));
    }

    if (aws.getRegion() != null) {
      return aws.getRegion();
    } else if (domain != null) {
      return new DefaultAwsRegionProviderChain().getRegion();
    } else {
      String awsRegion = regionFromAwsUrls(hosts);
      if (awsRegion == null) throw new IllegalArgumentException("Couldn't find region in " + hosts);
      return awsRegion;
    }
  }

  /** By default, get credentials from the {@link DefaultAWSCredentialsProviderChain} */
  @Bean
  @ConditionalOnMissingBean
  AWSCredentials.Provider credentials() {
    return new AWSCredentials.Provider() {
      AWSCredentialsProvider delegate = new DefaultAWSCredentialsProviderChain();

      @Override
      public AWSCredentials get() {
        com.amazonaws.auth.AWSCredentials result = delegate.getCredentials();
        String sessionToken =
            result instanceof AWSSessionCredentials
                ? ((AWSSessionCredentials) result).getSessionToken()
                : null;
        return new AWSCredentials(
            result.getAWSAccessKeyId(), result.getAWSSecretKey(), sessionToken);
      }
    };
  }

  /**
   * When the domain variable is set, we lookup the elasticsearch domain url dynamically, using the
   * AWS api. Otherwise, we assume the URL specified in ES_HOSTS is correct.
   */
  @Bean
  @Conditional(AwsDomainSetCondition.class)
  StorageComponent storage(
      String region,
      ElasticsearchStorage.Builder builder,
      ZipkinElasticsearchAwsStorageProperties aws,
      @Qualifier("zipkinElasticsearchHttp") OkHttpClient client) {
    String domain = aws.getDomain();

    ElasticsearchDomainEndpoint hosts =
        new ElasticsearchDomainEndpoint(
            client, HttpUrl.parse("https://es." + region + ".amazonaws.com"), domain);

    return builder.hostsSupplier(hosts).build();
  }

  static final class AwsDomainSetCondition extends SpringBootCondition {
    static final String PROPERTY_NAME = "zipkin.storage.elasticsearch.aws.domain";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata a) {
      String domain = context.getEnvironment().getProperty(PROPERTY_NAME);
      return domain == null || domain.isEmpty()
          ? ConditionOutcome.noMatch(PROPERTY_NAME + " isn't set")
          : ConditionOutcome.match();
    }
  }

  static final class AwsMagic implements Condition {
    @Override
    public boolean matches(ConditionContext condition, AnnotatedTypeMetadata md) {
      String hosts = condition.getEnvironment().getProperty("zipkin.storage.elasticsearch.hosts");
      String domain =
          condition.getEnvironment().getProperty("zipkin.storage.elasticsearch.aws.domain");

      // If neither hosts nor domain, no AWS magic
      if (isEmpty(hosts) && isEmpty(domain)) return false;

      // Either we have a domain, or we check the hosts auto-detection magic
      return !isEmpty(domain) || regionFromAwsUrls(hosts) != null;
    }
  }

  static String regionFromAwsUrls(String hosts) {
    String awsRegion = null;
    for (String url : hosts.split(",", 100)) {
      Matcher matcher = AWS_URL.matcher(url);
      if (matcher.find()) {
        String matched = matcher.group(1);
        if (awsRegion != null)
          throw new IllegalArgumentException(
              "too many regions: saw '" + awsRegion + "' and '" + matched + "'");
        awsRegion = matcher.group(1);
      } else {
        if (awsRegion != null)
          throw new IllegalArgumentException(
              "mismatched regions; saw '" + awsRegion + "' but none found in '" + url + "'");
      }
    }
    return awsRegion;
  }

  private static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }
}
