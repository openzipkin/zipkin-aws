/**
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
package brave.instrumentation.aws;

import brave.test.http.ITHttpClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import org.junit.Ignore;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class ITAwsClientTracing_SyncClient extends ITHttpClient<FakeSyncClient> {

  @Override protected FakeSyncClient newClient(int port) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setMaxErrorRetry(2);
    clientConfiguration.setRequestTimeout(1000);

    FakeSyncClient.Builder clientBuilder = new FakeSyncClient.Builder()
        .withEndpointConfiguration(
            new EndpointConfiguration("http://127.0.0.1:" + port, "us-east-1"))
        .withClientConfiguration(clientConfiguration);

    return client = AwsClientTracing.create(httpTracing).build(clientBuilder);
  }

  @Override protected void closeClient(FakeSyncClient client) {
    // TODO
  }

  @Override protected void get(FakeSyncClient client, String pathIncludingQuery) {
    client.get(pathIncludingQuery);
  }

  @Override protected void post(FakeSyncClient client, String pathIncludingQuery, String body) {
    client.post(pathIncludingQuery, body);
  }

  @Override public void makesChildOfCurrentSpan() throws Exception {
    super.makesChildOfCurrentSpan();
    assertApplicationSpan();
  }

  @Override public void supportsPortableCustomization() throws Exception {
    super.supportsPortableCustomization();
    assertApplicationSpan();
  }

  @Override public void defaultSpanNameIsMethodName() throws Exception {
    super.defaultSpanNameIsMethodName();
    assertApplicationSpan();
  }

  @Override public void propagatesSpan() throws Exception {
    super.propagatesSpan();
    assertApplicationSpan();
  }

  @Override public void propagatesExtra_newTrace() throws Exception {
    super.propagatesExtra_newTrace();
    assertApplicationSpan();
  }

  @Override public void post() throws Exception {
    super.post();
    assertApplicationSpan();
  }

  @Override public void httpPathTagExcludesQueryParams() throws Exception {
    super.httpPathTagExcludesQueryParams();
    assertApplicationSpan();
  }

  @Override public void reportsClientKindToZipkin() throws Exception {
    super.reportsClientKindToZipkin();
    assertApplicationSpan();
  }

  @Override public void reportsServerAddress() throws Exception {
    // TODO: figure out if we can read the actual connection
  }

  @Override public void reportsSpanOnTransportException() throws Exception {
    // TODO: figure out what's up with error reporting
  }

  @Override public void addsErrorTagOnTransportException() throws Exception {
    // TODO: figure out what's up with error reporting
  }

  @Override public void addsStatusCodeWhenNotOk() throws Exception {
    // TODO: figure out what's up with error reporting
  }

  @Override public void redirect() throws Exception {
    // TODO: figure out what's up with redirects
  }

  void assertApplicationSpan() throws InterruptedException {
    assertThat(takeSpan().kind())
        .withFailMessage("Expected application span")
        .isNull();
  }
}
