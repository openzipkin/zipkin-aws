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

import brave.test.http.ITHttpAsyncClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import java.util.Collections;

public class ITTracingRequestHandler extends ITHttpAsyncClient<AmazonDynamoDB> {
  @Override protected void getAsync(AmazonDynamoDB dynamoDB, String s) throws Exception {
    dynamoDB.getItem(s, Collections.EMPTY_MAP);
  }

  @Override protected AmazonDynamoDB newClient(int i) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setMaxErrorRetry(2);
    clientConfiguration.setRequestTimeout(1000);

    return AmazonDynamoDBAsyncClientBuilder.standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + i, "us-east-1"))
        .withRequestHandlers(TracingRequestHandler.create(httpTracing))
        .withClientConfiguration(clientConfiguration)
        .build();
  }

  @Override protected void closeClient(AmazonDynamoDB dynamoDB) throws Exception {

  }

  @Override protected void get(AmazonDynamoDB dynamoDB, String s) throws Exception {
    dynamoDB.getItem("s", Collections.EMPTY_MAP);
  }

  @Override protected void post(AmazonDynamoDB dynamoDB, String s, String s1) throws Exception {
    dynamoDB.getItem("s", Collections.EMPTY_MAP);
  }


  /*
   * Tests that don't work
   */

  /** AWS doesn't use redirect */
  @Override public void redirect() {}

  /** Span name doesn't conform to expectation */
  @Override public void supportsPortableCustomization() {}

  /** Paths don't conform to expectation, always / */
  @Override public void customSampler() {}

  /** Body's inherently have a structure */
  @Override public void post() {}

  /** Unable to parse remote endpoint */
  @Override public void reportsServerAddress() {}

  /** Path is always empty */
  @Override public void httpPathTagExcludesQueryParams() {}

  /** Error has exception instead of code */
  @Override public void addsStatusCodeWhenNotOk() {}

  /** All http methods are POST */
  @Override public void defaultSpanNameIsMethodName() {}
}
