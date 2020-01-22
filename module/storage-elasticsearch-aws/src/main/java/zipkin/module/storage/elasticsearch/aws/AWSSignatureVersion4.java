/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
package zipkin.module.storage.elasticsearch.aws;

import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.client.SimpleDecoratingHttpClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.AsciiString;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import static com.linecorp.armeria.common.HttpHeaderNames.AUTHORITY;
import static com.linecorp.armeria.common.HttpHeaderNames.HOST;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

// http://docs.aws.amazon.com/general/latest/gr/signature-version-4.html
final class AWSSignatureVersion4 extends SimpleDecoratingHttpClient {
  static final String EMPTY_STRING_HASH =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final AsciiString X_AMZ_DATE = HttpHeaderNames.of("x-amz-date");
  static final AsciiString X_AMZ_SECURITY_TOKEN = HttpHeaderNames.of("x-amz-security-token");
  static final AsciiString[] OTHER_CANONICAL_HEADERS = {X_AMZ_DATE, X_AMZ_SECURITY_TOKEN};
  static final String HOST_DATE = HOST + ";" + X_AMZ_DATE;
  static final String HOST_DATE_TOKEN = HOST_DATE + ";" + X_AMZ_SECURITY_TOKEN;
  static final String SERVICE = "es";
  static final byte[] SERVICE_BYTES = {'e', 's'};
  static final byte[] AWS4_REQUEST = "aws4_request".getBytes(UTF_8);

  static Function<HttpClient, HttpClient> newDecorator(String region,
      AWSCredentials.Provider credentials) {
    return client -> new AWSSignatureVersion4(client, region, credentials);
  }

  // SimpleDateFormat isn't thread-safe
  static final ThreadLocal<SimpleDateFormat> iso8601 = ThreadLocal.withInitial(() -> {
    SimpleDateFormat result = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
    result.setTimeZone(TimeZone.getTimeZone("UTC"));
    return result;
  });

  final String region;
  final byte[] regionBytes;
  final AWSCredentials.Provider credentials;

  AWSSignatureVersion4(HttpClient delegate, String region, AWSCredentials.Provider credentials) {
    super(delegate);
    if (region == null) throw new NullPointerException("region == null");
    if (credentials == null) throw new NullPointerException("credentials == null");
    this.region = region;
    this.regionBytes = region.getBytes(UTF_8);
    this.credentials = credentials;
  }

  @Override public HttpResponse execute(ClientRequestContext ctx, HttpRequest req) {
    // We aggregate the request body with pooled objects because signing implies reading it before
    // sending it to Elasticsearch.
    return HttpResponse.from(
        req.aggregateWithPooledObjects(ctx.contextAwareEventLoop(), ctx.alloc())
            .thenApply(aggReg -> {
              try {
                AggregatedHttpRequest signed = sign(ctx, aggReg);
                return delegate().execute(ctx, signed.toHttpRequest());
              } catch (Exception e) {
                return HttpResponse.ofFailure(e);
              }
            }));
  }

  static void writeCanonicalString(
      ClientRequestContext ctx, RequestHeaders headers, HttpData payload, ByteBuf result) {
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

    writeCanonicalHeaderValue(HOST, host(headers, ctx), signedHeaders, result);
    try {
      for (AsciiString canonicalHeader : OTHER_CANONICAL_HEADERS) {
        String value = headers.get(canonicalHeader);
        if (value != null) {
          writeCanonicalHeaderValue(canonicalHeader, value, signedHeaders, result);
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
    if (!payload.isEmpty()) {
      ByteBufUtil.writeUtf8(result, ByteBufUtil.hexDump(sha256(payload)));
    } else {
      ByteBufUtil.writeUtf8(result, EMPTY_STRING_HASH);
    }
  }

  private static void writeCanonicalHeaderValue(AsciiString canonicalHeader, String value,
      ByteBuf signedHeaders, ByteBuf result) {
    ByteBufUtil.writeUtf8(result, canonicalHeader);
    result.writeByte(':');
    ByteBufUtil.writeUtf8(result, value);
    result.writeByte('\n');

    signedHeaders.writeByte(';');
    ByteBufUtil.writeUtf8(signedHeaders, canonicalHeader);
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
    String credentialScope = credentialScope(yyyyMMdd, region);

    RequestHeadersBuilder builder = req.headers().toBuilder()
        .set(X_AMZ_DATE, timestamp);

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

      String authorization = "AWS4-HMAC-SHA256 Credential="
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

  static String credentialScope(String yyyyMMdd, String region) {
    return format("%s/%s/%s/%s", yyyyMMdd, region, SERVICE, "aws4_request");
  }

  byte[] signatureKey(String secretKey, String yyyyMMdd) {
    byte[] kSecret = ("AWS4" + secretKey).getBytes(UTF_8);

    byte[] kDate = hmacSha256(kSecret, yyyyMMdd.getBytes(UTF_8));
    byte[] kRegion = hmacSha256(kDate, regionBytes);
    byte[] kService = hmacSha256(kRegion, SERVICE_BYTES);
    return hmacSha256(kService, AWS4_REQUEST);
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

  // Gets the host for use in signing. This requires a ceremony because Armeria doesn't reliably
  // provide this information.
  //
  // - Depending on whether the request comes from a health check or normal, ctx.endpoint().host()
  //   may be an IP address, while the actual host is in either HOST or AUTHORITY additional
  //   headers.
  //
  // - The host extracted from additional headers usually has a port attached, even for well-defined
  //   ones like HTTPS:443. Armeria strips this off in the end before sending the request it seems
  //   so we need to make sure to strip it here too since we always use port 443 for AWS ES.
  static String host(RequestHeaders headers, ClientRequestContext ctx) {
    String host = headers.get(AUTHORITY);
    if (host == null) {
      host = ctx.additionalRequestHeaders().get(HttpHeaderNames.AUTHORITY);
    }
    if (host == null) {
      host = ctx.endpoint().host();
    }
    int colonIndex = host.indexOf(':');
    if (colonIndex >= 0) {
      host = host.substring(0, colonIndex);
    }

    return host;
  }
}
