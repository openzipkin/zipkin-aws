package brave.instrumentation.aws;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.client.AwsSyncClientParams;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.HttpResponseHandler;
import java.io.ByteArrayInputStream;
import java.net.URI;

class FakeSyncClient extends AmazonWebServiceClient {
  static class Builder extends AwsSyncClientBuilder<Builder, FakeSyncClient> {

    Builder() {
      super(new ClientConfigurationFactory());
    }

    @Override protected FakeSyncClient build(AwsSyncClientParams clientParams) {
      return new FakeSyncClient(getSyncClientParams());
    }
  }

  FakeSyncClient(AwsSyncClientParams clientConfiguration) {
    super(clientConfiguration);
    setServiceNameIntern("fake");
  }

  void post(String pathIncludingQuery, String body) {
    invoke(HttpMethodName.POST, pathIncludingQuery, body);
  }

  void get(String pathIncludingQuery) {
    invoke(HttpMethodName.GET, pathIncludingQuery, null);
  }

  HttpResponse invoke(HttpMethodName httpMethod, String pathIncludingQuery, String body) {
    AmazonWebServiceRequest serviceRequest = new AmazonWebServiceRequest() {
    };
    serviceRequest = beforeClientExecution(serviceRequest);

    Request<AmazonWebServiceRequest> request =
        new DefaultRequest<>(serviceRequest, getServiceName());
    request.setHttpMethod(httpMethod);
    URI uri = URI.create(pathIncludingQuery);
    request.setResourcePath(uri.getPath());
    if (uri.getQuery() != null) {
      for (String entry : uri.getQuery().split("&", -1)) {
        int equals = entry.indexOf('=');
        request.addParameter(entry.substring(0, equals), entry.substring(equals + 1));
      }
    }
    if (body != null) {
      request.setContent(new ByteArrayInputStream(body.getBytes()));
    }
    request.setEndpoint(endpoint);
    request.setTimeOffset(timeOffset);

    return client.requestExecutionBuilder()
        .request(request)
        .executionContext(createExecutionContext(serviceRequest))
        .execute(new HttpResponseHandler<String>() {
          @Override public String handle(HttpResponse response) {
            return "";
          }

          @Override public boolean needsConnectionLeftOpen() {
            return false;
          }
        }).getHttpResponse();
  }
}
