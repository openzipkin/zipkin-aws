package brave.instrumentation.awsv2;

import brave.http.HttpClientAdapter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

public class HttpAdapter extends HttpClientAdapter<SdkHttpRequest.Builder, SdkHttpResponse> {
  @Override public String method(SdkHttpRequest.Builder builder) {
    return builder.method().name();
  }

  @Override public String path(SdkHttpRequest.Builder request) {
    return request.encodedPath();
  }

  @Override public String url(SdkHttpRequest.Builder request) {
    StringBuilder url = new StringBuilder(request.protocol())
        .append("://")
        .append(request.host());
    if (request.encodedPath() != null) url.append(request.encodedPath());
    if (request.rawQueryParameters().isEmpty()) return url.toString();
    url.append('?');
    Iterator<Map.Entry<String, List<String>>> entries = request.rawQueryParameters().entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, List<String>> entry = entries.next();
      url.append(entry.getKey());
      if (entry.getKey().isEmpty()) continue;
      url.append('=').append(entry.getValue().get(0)); // skip the others.
      if (entries.hasNext()) url.append('&');
    }
    return url.toString();
  }

  @Override public String requestHeader(SdkHttpRequest.Builder builder, String s) {
    return builder.headers().get(s).get(0);
  }

  @Override public Integer statusCode(SdkHttpResponse sdkHttpResponse) {
    return sdkHttpResponse.statusCode();
  }
}
