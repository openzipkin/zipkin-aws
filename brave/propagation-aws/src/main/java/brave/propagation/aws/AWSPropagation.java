/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.propagation.aws;

import brave.Tracing;
import brave.baggage.BaggageField;
import brave.internal.Nullable;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static brave.propagation.aws.AWSExtractor.EMPTY;
import static brave.propagation.aws.HexCodec.writeHexByte;
import static brave.propagation.aws.HexCodec.writeHexLong;

/**
 * Utility for working with Amazon Web Services Trace IDs, for example reading from headers or
 * environment variables. {@code x-amzn-trace-id} is primarily for Amazon's X-Ray service, but it is
 * also integrated with AWS ALB, API Gateway and Lambda.
 *
 * <p>For example, if you are in a lambda environment, you can read the incoming context like this:
 *
 * <pre>{@code
 * span = tracer.nextSpan(AWSPropagation.extractLambda());
 * }</pre>
 *
 * <h3>Details</h3>
 * <p>
 * {@code x-amzn-trace-id} (and the lambda equivalent {@code _X_AMZN_TRACE_ID}) follows RFC 6265
 * style syntax (https://tools.ietf.org/html/rfc6265#section-2.2): fields are split on semicolon and
 * optional whitespace.
 *
 * <p>Description of the {@code Root} (or {@code Self}) field from AWS CLI help:
 *
 * <p>A trace_id consists of three numbers separated by hyphens. For example, {@code
 * 1-58406520-a006649127e371903a2de979}. This includes:
 *
 * <pre>
 * <ul>
 * <li>The version number, i.e. 1</li>
 * <li>The time of the original request, in Unix epoch time, in 8  hexadecimal digits. For example,
 * 10:00AM December 2nd, 2016 PST in epoch time is 1480615200 seconds, or 58406520 in
 * hexadecimal.</li>
 * <li>A 96-bit identifier for the trace, globally unique, in 24 hexadecimal digits.</li>
 * </ul>
 * </pre>
 */
public final class AWSPropagation implements Propagation<String> {
  // Using lowercase field name as http is case-insensitive, but http/2 transport downcases */
  static final String AMZN_TRACE_ID_NAME = "x-amzn-trace-id";
  static final BaggageField FIELD_AMZN_TRACE_ID = BaggageField.create(AMZN_TRACE_ID_NAME);
  static final Propagation<String> INSTANCE = new AWSPropagation();
  /** When present, this context was created with AWSPropagation */
  static final AmznTraceId NO_CUSTOM_FIELDS = new AmznTraceId("");
  static final Extractor<String> STRING_EXTRACTOR =
      INSTANCE.extractor((request, key) -> request);
  public static final Propagation.Factory FACTORY = new Factory();

  static final char[] ROOT = "Root=".toCharArray();
  static final char[] PARENT = ";Parent=".toCharArray();
  static final char[] SAMPLED = ";Sampled=".toCharArray();
  static final int ROOT_LENGTH = 35;

  static final class Factory extends Propagation.Factory {
    @Override public Propagation<String> get() {
      return INSTANCE;
    }

    @Override public boolean supportsJoin() {
      return false;
    }

    @Override public boolean requires128BitTraceId() {
      return true;
    }

    @Override public String toString() {
      return "AWSPropagationFactory";
    }
  }

  final List<String> keyNames;

  AWSPropagation() {
    this.keyNames = Collections.unmodifiableList(Arrays.asList(AMZN_TRACE_ID_NAME));
  }

  /** returns the name of the header field: "x-amzn-trace-id" */
  @Override
  public List<String> keys() {
    return keyNames;
  }

  @Override
  public <R> Injector<R> injector(Setter<R, String> setter) {
    if (setter == null) throw new NullPointerException("setter == null");
    return new AWSInjector<>(this, setter);
  }

  /** Returns the current {@link #traceId(TraceContext)} or null if not available */
  @Nullable
  public static String currentTraceId() {
    Tracing tracing = Tracing.current();
    if (tracing == null) return null;
    TraceContext context = tracing.currentTraceContext().get();
    if (context == null) return null;
    return traceId(context);
  }

  /**
   * Used for log correlation or {@link brave.Span#tag(String, String) tag values}
   *
   * @return a formatted Root field like "1-58406520-a006649127e371903a2de979" or null if the
   * context was not created from an instance of {@link AWSPropagation}.
   */
  @Nullable
  public static String traceId(TraceContext context) {
    for (int i = 0, length = context.extra().size(); i < length; i++) {
      Object next = context.extra().get(i);
      if (next instanceof AmznTraceId) {
        char[] result = new char[ROOT_LENGTH];
        writeRoot(context, result, 0);
        return new String(result);
      }
    }
    // See if we have the field as a pass-through
    String maybeHeader = FIELD_AMZN_TRACE_ID.getValue(context);
    if (maybeHeader == null) return null;
    int i = maybeHeader.indexOf("Root=");
    if (i == -1) return null;
    i += 5; // Root=
    if (maybeHeader.length() < i + ROOT_LENGTH) return null;
    return maybeHeader.substring(i, i + ROOT_LENGTH);
  }

  /** Writes 35 characters representing the input trace ID to the buffer at the given offset */
  static void writeRoot(TraceContext context, char[] result, int offset) {
    result[offset] = '1'; // version
    result[offset + 1] = '-'; // delimiter
    long high = context.traceIdHigh();
    writeHexByte(result, offset + 2, (byte) ((high >>> 56L) & 0xff));
    writeHexByte(result, offset + 4, (byte) ((high >>> 48L) & 0xff));
    writeHexByte(result, offset + 6, (byte) ((high >>> 40L) & 0xff));
    writeHexByte(result, offset + 8, (byte) ((high >>> 32L) & 0xff));
    result[offset + 10] = '-';
    writeHexByte(result, offset + 11, (byte) ((high >>> 24L) & 0xff));
    writeHexByte(result, offset + 13, (byte) ((high >>> 16L) & 0xff));
    writeHexByte(result, offset + 15, (byte) ((high >>> 8L) & 0xff));
    writeHexByte(result, offset + 17, (byte) (high & 0xff));
    writeHexLong(result, offset + 19, context.traceId());
  }

  @Override
  public <R> Extractor<R> extractor(Getter<R, String> getter) {
    if (getter == null) throw new NullPointerException("getter == null");
    return new AWSExtractor<>(this, getter);
  }

  static final class AmznTraceId { // hidden intentionally
    final CharSequence customFields;

    AmznTraceId(CharSequence customFields) {
      this.customFields = customFields;
    }

    @Override public String toString() {
      if (customFields.length() == 0) return "AmznTraceId{}";
      return "AmznTraceId{customFields=" + customFields + "}";
    }
  }

  /**
   * This is used for extracting from the AWS lambda environment variable {@code _X_AMZN_TRACE_ID}.
   *
   * @see #extract(String)
   */
  public static TraceContextOrSamplingFlags extractLambda() {
    return extract(System.getenv("_X_AMZN_TRACE_ID"));
  }

  /** Like {@link Extractor#extract(Object)} except reading from a single field. */
  public static TraceContextOrSamplingFlags extract(String amznTraceId) {
    if (amznTraceId == null) return EMPTY;
    return STRING_EXTRACTOR.extract(amznTraceId);
  }
}
