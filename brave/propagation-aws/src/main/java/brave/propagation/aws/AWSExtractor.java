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
package brave.propagation.aws;

import brave.propagation.Propagation.Getter;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.propagation.TraceIdContext;
import brave.propagation.aws.AWSPropagation.AmznTraceId;

import static brave.propagation.aws.AWSPropagation.EXTRA_MARKER;
import static brave.propagation.aws.AWSPropagation.ROOT_LENGTH;
import static brave.propagation.aws.AWSPropagation.AMZN_TRACE_ID_NAME;

/**
 * Fields defined by Amazon:
 * <ul>
 *   <li>Root - the first instrumented segment</li>
 *   <li>Parent - the last instrumented segment</li>
 *   <li>Sampled - '1', '0' or '?'</li>
 *   <li>Self - inserted by elasticloadbalancing (we drop these)</li>
 * </ul>
 *
 * <p>See https://docs.aws.amazon.com/xray/latest/devguide/xray-concepts.html
 * <p>See https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-request-tracing.html
 */
final class AWSExtractor<R> implements Extractor<R> {
  static final TraceContextOrSamplingFlags EMPTY =
      TraceContextOrSamplingFlags.EMPTY.toBuilder().addExtra(EXTRA_MARKER).build();

  final AWSPropagation propagation;
  final Getter<R, String> getter;

  AWSExtractor(AWSPropagation propagation, Getter<R, String> getter) {
    this.propagation = propagation;
    this.getter = getter;
  }

  enum Op {
    SKIP,
    ROOT,
    PARENT,
    SAMPLED,
    EXTRA
  }

  @Override
  public TraceContextOrSamplingFlags extract(R request) {
    if (request == null) throw new NullPointerException("request == null");
    String traceIdString = getter.get(request, AMZN_TRACE_ID_NAME);
    if (traceIdString == null) return EMPTY;

    Boolean sampled = null;
    long traceIdHigh = 0L, traceId = 0L;
    Long parent = null;
    StringBuilder currentString = new StringBuilder(7 /* Sampled.length */), extraFields = null;
    Op op = null;
    OUTER:
    for (int i = 0, length = traceIdString.length(); i < length; i++) {
      char c = traceIdString.charAt(i);
      if (c == ' ') continue; // trim whitespace
      if (c == '=') { // we reached a field name
        if (++i == length) break; // skip '=' character
        if (currentString.indexOf("Root") == 0) {
          op = Op.ROOT;
        } else if (currentString.indexOf("Parent") == 0) {
          op = Op.PARENT;
        } else if (currentString.indexOf("Sampled") == 0) {
          op = Op.SAMPLED;
        } else if (currentString.indexOf("Self") == 0) {
          // ALB implements Trace ID chaining using self so that customers not using X-Ray
          // (I.e. request logs) can do the correlation themselves. We drop these
          op = Op.SKIP;
        } else {
          op = Op.EXTRA;
          if (extraFields == null) extraFields = new StringBuilder();
          extraFields.append(';').append(currentString);
        }
        currentString.setLength(0);
      } else if (op == null) {
        currentString.append(c);
        continue;
      }
      // no longer whitespace
      switch (op) {
        case EXTRA:
          extraFields.append(c);
          while (i < length && (c = traceIdString.charAt(i)) != ';') {
            extraFields.append(c);
            i++;
          }
          break;
        case SKIP:
          while (++i < length && traceIdString.charAt(i) != ';') {
            // skip until we hit a delimiter
          }
          break;
        case ROOT:
          if (i + ROOT_LENGTH > length // 35 = length of 1-67891233-abcdef012345678912345678
              || traceIdString.charAt(i++) != '1'
              || traceIdString.charAt(i++) != '-') {
            break OUTER; // invalid version or format
          }
          // Parse the epoch seconds and high 32 of the 96 bit trace ID into traceID high
          for (int hyphenIndex = i + 8, endIndex = hyphenIndex + 1 + 8; i < endIndex; i++) {
            c = traceIdString.charAt(i);
            if (c == '-' && i == hyphenIndex) continue; // skip delimiter between epoch and random
            traceIdHigh <<= 4;
            if (c >= '0' && c <= '9') {
              traceIdHigh |= c - '0';
            } else if (c >= 'a' && c <= 'f') {
              traceIdHigh |= c - 'a' + 10;
            } else {
              break OUTER; // invalid format
            }
          }
          // Parse the low 64 of the 96 bit trace ID into traceId
          for (int endIndex = i + 16; i < endIndex; i++) {
            c = traceIdString.charAt(i);
            traceId <<= 4;
            if (c >= '0' && c <= '9') {
              traceId |= c - '0';
            } else if (c >= 'a' && c <= 'f') {
              traceId |= c - 'a' + 10;
            } else {
              break OUTER; // invalid format
            }
          }
          break;
        case PARENT:
          long parentId = 0L;
          for (int endIndex = i + 16; i < endIndex; i++) {
            c = traceIdString.charAt(i);
            parentId <<= 4;
            if (c >= '0' && c <= '9') {
              parentId |= c - '0';
            } else if (c >= 'a' && c <= 'f') {
              parentId |= c - 'a' + 10;
            } else {
              break OUTER; // invalid format
            }
          }
          parent = parentId;
          break;
        case SAMPLED:
          c = traceIdString.charAt(i++);
          if (c == '1') {
            sampled = true;
          } else if (c == '0') {
            sampled = false;
          }
          break;
      }
      op = null;
    }

    AmznTraceId amznTraceId = EXTRA_MARKER;
    if (extraFields != null) {
      amznTraceId = new AmznTraceId();
      amznTraceId.customFields = extraFields;
    }

    if (traceIdHigh == 0L) { // traceIdHigh cannot be null, so just return sampled
      SamplingFlags samplingFlags = SamplingFlags.EMPTY;
      if (sampled != null) {
        samplingFlags = sampled ? SamplingFlags.SAMPLED : SamplingFlags.NOT_SAMPLED;
      }
      return TraceContextOrSamplingFlags.newBuilder(samplingFlags)
          .addExtra(amznTraceId)
          .build();
    } else if (parent == null) {
      return TraceContextOrSamplingFlags.newBuilder(TraceIdContext.newBuilder()
          .traceIdHigh(traceIdHigh)
          .traceId(traceId)
          .sampled(sampled)
          .build())
          .addExtra(amznTraceId)
          .build();
    }
    return TraceContextOrSamplingFlags.create(
        TraceContext.newBuilder()
            .traceIdHigh(traceIdHigh)
            .traceId(traceId)
            .spanId(parent)
            .sampled(sampled)
            .addExtra(amznTraceId)
            .build());
  }
}
