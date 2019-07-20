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
import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.SimpleDecoratingClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.squareup.moshi.JsonReader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.AsciiString;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import okio.Buffer;
import zipkin2.elasticsearch.ElasticsearchStorage.HostsSupplier;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.elasticsearch.internal.JsonReaders.enterPath;

// http://docs.aws.amazon.com/general/latest/gr/signature-version-4.html
final class AWSSignatureVersion4 extends SimpleDecoratingClient<HttpRequest, HttpResponse> {
  static final String EMPTY_STRING_HASH =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final AsciiString X_AMZ_DATE = HttpHeaderNames.of("x-amz-date");
  static final AsciiString X_AMZ_SECURITY_TOKEN = HttpHeaderNames.of("x-amz-security-token");
  static final AsciiString[] CANONICAL_HEADERS =
      {HttpHeaderNames.HOST, X_AMZ_DATE, X_AMZ_SECURITY_TOKEN};
  static final String HOST_DATE = HttpHeaderNames.HOST + ";" + X_AMZ_DATE;
  static final String HOST_DATE_TOKEN = HOST_DATE + ";" + X_AMZ_SECURITY_TOKEN;

  static final byte[] AWS4_REQUEST = "aws4_request".getBytes(UTF_8);

  static Function<Client<HttpRequest, HttpResponse>, Client<HttpRequest, HttpResponse>>
  newDecorator(String region, HostsSupplier hostsSupplier, AWSCredentials.Provider credentials) {
    return client -> new AWSSignatureVersion4(client, region, hostsSupplier, credentials);
  }

  // SimpleDateFormat isn't thread-safe
  static final ThreadLocal<SimpleDateFormat> iso8601 = ThreadLocal.withInitial(() -> {
    SimpleDateFormat result = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
    result.setTimeZone(TimeZone.getTimeZone("UTC"));
    return result;
  });

  final String region;
  final byte[] regionBytes;
  final HostsSupplier hostsSupplier;
  final String service;
  final byte[] serviceBytes;
  final AWSCredentials.Provider credentials;

  AWSSignatureVersion4(Client<HttpRequest, HttpResponse> delegate, String region,
      HostsSupplier hostsSupplier, AWSCredentials.Provider credentials) {
    super(delegate);
    if (region == null) throw new NullPointerException("region == null");
    if (credentials == null) throw new NullPointerException("credentials == null");
    this.region = region;
    this.regionBytes = region.getBytes(UTF_8);
    this.hostsSupplier = hostsSupplier;
    this.service = "es";
    this.serviceBytes = service.getBytes(UTF_8);
    this.credentials = credentials;
  }

  volatile String host;

  String host(){
    if (host == null) {
      synchronized (this) {
        if (host == null) {
          // We currently cannot access the hostname during decoration. Instead, we proceed knowing
          // that our implementation of AWS ES only returns a single endpoint.
          List<String> hosts = hostsSupplier.get();
          if (hosts.isEmpty()) throw new RuntimeException("No AWS ES hosts!");
          if (hosts.size() > 1) throw new RuntimeException("Expected one AWS ES host: " + hosts);
          host = URI.create(hosts.get(0)).getHost();
        }
      }
    }
    return host;
  }

  @Override public HttpResponse execute(ClientRequestContext ctx, HttpRequest req) {
    return HttpResponse.from(
        req.aggregateWithPooledObjects(ctx.eventLoop(), ctx.alloc())
            .thenCompose(aggReg -> {
              try {
                AggregatedHttpRequest signed = sign(ctx, aggReg);
                return delegate().execute(ctx, HttpRequest.of(signed)).aggregate();
              } catch (Exception e) {
                CompletableFuture<AggregatedHttpResponse> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
              }
            })
            .thenApply(aggResp -> {
              if (!aggResp.status().equals(HttpStatus.FORBIDDEN)) {
                return HttpResponse.of(aggResp);
              }
              try {
                JsonReader message = enterPath(
                    JsonReader.of(new Buffer().write(aggResp.content().array())), "message");
                if (message != null) throw new IllegalStateException(message.nextString());
              } catch (IOException e) {
                // Ignore JSON parse failure.
              }
              throw new IllegalStateException(aggResp.contentUtf8());
            }));
  }

  static void writeCanonicalString(
      ClientRequestContext ctx, RequestHeaders headers, HttpData content, ByteBuf result) {
    // HTTPRequestMethod + '\n' +
    ByteBufUtil.writeUtf8(result, ctx.method().name());
    result.writeByte('\n');

    // CanonicalURI + '\n' +
    // TODO: make this more efficient
    ByteBufUtil.writeUtf8(result,
        ctx.path().replace("*", "%2A").replace(",", "%2C").replace(":", "%3A"));
    result.writeByte('\n');

    // CanonicalQueryString + '\n' +
    String query = ctx.query();
    if (query != null) {
      ByteBufUtil.writeUtf8(result, query);
    }
    result.writeByte('\n');

    // CanonicalHeaders + '\n' +
    ByteBuf signedHeaders = ctx.alloc().buffer();
    try {
      for (AsciiString canonicalHeader : CANONICAL_HEADERS) {
        String value = headers.get(canonicalHeader);
        if (value != null) {
          ByteBufUtil.writeUtf8(result, canonicalHeader);
          result.writeByte(':');
          ByteBufUtil.writeUtf8(result, value);
          result.writeByte('\n');

          signedHeaders.writeByte(';');
          ByteBufUtil.writeUtf8(signedHeaders, canonicalHeader);
        }
      }
      result.writeByte('\n'); // end headers

      // SignedHeaders + '\n' +
      signedHeaders.readByte(); // throw away the first semicolon
      result.writeBytes(signedHeaders);
    } finally {
      signedHeaders.release();
    }
    result.writeByte('\n');

    // HexEncode(Hash(Payload))
    if (!content.isEmpty()) {
      ByteBufUtil.writeUtf8(result, ByteBufUtil.hexDump(sha256(content)));
    } else {
      ByteBufUtil.writeUtf8(result, EMPTY_STRING_HASH);
    }
  }

  static void writeToSign(
      String timestamp, String credentialScope, ByteBuf canonicalRequest, ByteBuf result) {
    // Algorithm + '\n' +
    ByteBufUtil.writeUtf8(result, "AWS4-HMAC-SHA256\n");
    // RequestDate + '\n' +
    ByteBufUtil.writeUtf8(result, timestamp);
    result.writeByte('\n');
    // CredentialScope + '\n' +
    ByteBufUtil.writeUtf8(result, credentialScope);
    result.writeByte('\n');
    // HexEncode(Hash(CanonicalRequest))
    ByteBufUtil.writeUtf8(result, ByteBufUtil.hexDump(sha256(canonicalRequest.nioBuffer())));
  }

  static byte[] sha256(HttpData data) {
    final ByteBuffer buf;
    if (data instanceof ByteBufHolder) {
      buf = ((ByteBufHolder) data).content().nioBuffer();
    } else {
      buf = ByteBuffer.wrap(data.array());
    }
    return sha256(buf);
  }

  static byte[] sha256(ByteBuffer buf) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      messageDigest.update(buf);
      return messageDigest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError();
    }
  }

  AggregatedHttpRequest sign(ClientRequestContext ctx, AggregatedHttpRequest req) {
    AWSCredentials credentials = this.credentials.get();
    if (credentials == null) throw new NullPointerException("credentials == null");

    String timestamp = iso8601.get().format(new Date());
    String yyyyMMdd = timestamp.substring(0, 8);

    String credentialScope = format("%s/%s/%s/%s", yyyyMMdd, region, service, "aws4_request");

    RequestHeadersBuilder builder = req.headers().toBuilder()
        .set(X_AMZ_DATE, timestamp)
        .set(HttpHeaderNames.HOST, host());

    if (credentials.sessionToken != null) {
      builder.set(X_AMZ_SECURITY_TOKEN, credentials.sessionToken);
    }

    String signedHeaders = credentials.sessionToken == null ? HOST_DATE : HOST_DATE_TOKEN;
    ByteBuf canonicalString = ctx.alloc().heapBuffer();
    ByteBuf toSign = ctx.alloc().heapBuffer();
    try {
      writeCanonicalString(ctx, builder.build(), req.content(), canonicalString);
      writeToSign(timestamp, credentialScope, canonicalString, toSign);

      // TODO: this key is invalid when the secret key or the date change. both are very infrequent
      byte[] signatureKey = signatureKey(credentials.secretKey, yyyyMMdd);
      String signature = ByteBufUtil.hexDump(hmacSha256(signatureKey, toSign.nioBuffer()));

      String authorization =
          "AWS4-HMAC-SHA256 Credential="
              + credentials.accessKey
              + '/'
              + credentialScope
              + ", SignedHeaders="
              + signedHeaders
              + ", Signature="
              + signature;

      return AggregatedHttpRequest.of(
          builder.add(HttpHeaderNames.AUTHORIZATION, authorization).build(),
          req.content(),
          req.trailers());
    } finally {
      canonicalString.release();
      toSign.release();
    }
  }

  byte[] signatureKey(String secretKey, String yyyyMMdd) {
    byte[] kSecret = ("AWS4" + secretKey).getBytes(UTF_8);

    byte[] kDate = hmacSha256(kSecret, yyyyMMdd.getBytes(UTF_8));
    byte[] kRegion = hmacSha256(kDate, regionBytes);
    byte[] kService = hmacSha256(kRegion, serviceBytes);
    byte[] kSigning = hmacSha256(kService, AWS4_REQUEST);

    return kSigning;
  }

  static byte[] hmacSha256(byte[] secret, byte[] payload) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret, "HmacSHA256"));
      mac.update(payload);
      return mac.doFinal();
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError();
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }
  }

  static byte[] hmacSha256(byte[] secret, ByteBuffer payload) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret, "HmacSHA256"));
      mac.update(payload);
      return mac.doFinal();
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError();
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
