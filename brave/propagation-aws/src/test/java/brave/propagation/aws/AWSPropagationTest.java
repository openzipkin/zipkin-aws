/*
 * Copyright 2016-2024 The OpenZipkin Authors
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

import brave.Tracing;
import brave.baggage.BaggagePropagation;
import brave.baggage.BaggagePropagationConfig;
import brave.propagation.B3Propagation;
import brave.propagation.CurrentTraceContext;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.propagation.TraceIdContext;
import brave.propagation.aws.AWSPropagation.AmznTraceId;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static brave.internal.codec.HexCodec.lowerHexToUnsignedLong;
import static org.assertj.core.api.Assertions.assertThat;

class AWSPropagationTest {
  Map<String, String> carrier = new LinkedHashMap<>();
  Injector<Map<String, String>> injector = AWSPropagation.FACTORY.get().injector(Map::put);
  Extractor<Map<String, String>> extractor = AWSPropagation.FACTORY.get().extractor(Map::get);

  String sampledTraceId =
      "Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1";
  TraceContext sampledContext = TraceContext.newBuilder()
      .traceIdHigh(lowerHexToUnsignedLong("67891233abcdef01"))
      .traceId(lowerHexToUnsignedLong("2345678912345678"))
      .spanId(lowerHexToUnsignedLong("463ac35c9f6413ad"))
      .sampled(true)
      .addExtra(AWSPropagation.NO_CUSTOM_FIELDS)
      .build();

  @Test void traceId() {
    assertThat(AWSPropagation.traceId(sampledContext))
        .isEqualTo("1-67891233-abcdef012345678912345678");
  }

  @Test void traceIdWhenPassThrough() {
    carrier.put(
        "x-amzn-trace-id",
        "Robot=Hello;Self=1-582113d1-1e48b74b3603af8479078ed6;  "
            + "Root=1-58211399-36d228ad5d99923122bbe354;  "
            + "TotalTimeSoFar=112ms;CalledFrom=Foo");

    TraceContext context = contextWithPassThrough();

    assertThat(AWSPropagation.traceId(context)).isEqualTo("1-58211399-36d228ad5d99923122bbe354");
  }

  @Test void traceIdWhenPassThrough_nullOnTruncated() {
    carrier.put("x-amzn-trace-id", "Root=1-58211399-36d228ad5d99923122bbe3");

    TraceContext context = contextWithPassThrough();

    assertThat(AWSPropagation.traceId(context)).isNull();
  }

  TraceContext contextWithPassThrough() {
    extractor = BaggagePropagation.newFactoryBuilder(B3Propagation.FACTORY)
        .add(BaggagePropagationConfig.SingleBaggageField.remote(AWSPropagation.FIELD_AMZN_TRACE_ID))
        .build()
        .get()
        .extractor(Map::get);

    TraceContextOrSamplingFlags extracted = extractor.extract(carrier);

    // sanity check
    assertThat(extracted.samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
    assertThat(extracted.extra()).isNotEmpty();

    // Make a context that wasn't from AWSPropagation
    return TraceContext.newBuilder()
        .traceId(1L)
        .spanId(2L)
        .sampled(true)
        .addExtra(extracted.extra().get(0))
        .build();
  }

  @Test void traceId_null_if_not_aws() {
    TraceContext notAWS = sampledContext.toBuilder().clearExtra().build();
    assertThat(AWSPropagation.traceId(notAWS)).isNull();
  }

  @Test void currentTraceId() {
    try (Tracing t = Tracing.newBuilder().propagationFactory(AWSPropagation.FACTORY).build();
         CurrentTraceContext.Scope scope = t.currentTraceContext().newScope(sampledContext)) {
      assertThat(AWSPropagation.currentTraceId()).isEqualTo("1-67891233-abcdef012345678912345678");
    }
  }

  @Test void currentTraceId_null_if_no_current_context() {
    try (Tracing t = Tracing.newBuilder().propagationFactory(AWSPropagation.FACTORY).build()) {
      assertThat(AWSPropagation.currentTraceId()).isNull();
    }
  }

  @Test void currentTraceId_null_if_nothing_current() {
    assertThat(AWSPropagation.currentTraceId()).isNull();
  }

  @Test void inject() {
    injector.inject(sampledContext, carrier);

    assertThat(carrier).containsEntry("x-amzn-trace-id", sampledTraceId);
  }

  @Test void extract() {
    carrier.put("x-amzn-trace-id", sampledTraceId);

    assertThat(extractor.extract(carrier).context()).isEqualTo(sampledContext);
  }

  @Test void extract_containsMarker() {
    carrier.put("x-amzn-trace-id", sampledTraceId);

    TraceContextOrSamplingFlags extracted = extractor.extract(carrier);
    assertThat(extracted.context().extra()).containsExactly(AWSPropagation.NO_CUSTOM_FIELDS);
  }

  /** If invoked extract, a 128-bit trace ID will be created, compatible with AWS format */
  @Test void extract_fail_containsMarker() {
    TraceContextOrSamplingFlags extracted = extractor.extract(carrier);
    assertThat(extracted.extra()).containsExactly(AWSPropagation.NO_CUSTOM_FIELDS);
  }

  @Test void extract_static() {
    assertThat(AWSPropagation.extract(sampledTraceId).context()).isEqualTo(sampledContext);
  }

  @Test void extractDifferentOrder() {
    carrier.put(
        "x-amzn-trace-id",
        "Sampled=1;Parent=463ac35c9f6413ad;Root=1-67891233-abcdef012345678912345678");

    assertThat(extractor.extract(carrier).context()).isEqualTo(sampledContext);
  }

  @Test void extract_noParent() {
    carrier.put("x-amzn-trace-id", "Root=1-5759e988-bd862e3fe1be46a994272793;Sampled=1");

    assertThat(extractor.extract(carrier).traceIdContext())
        .isEqualTo(
            TraceIdContext.newBuilder()
                .traceIdHigh(lowerHexToUnsignedLong("5759e988bd862e3f"))
                .traceId(lowerHexToUnsignedLong("e1be46a994272793"))
                .sampled(true)
                .build());
  }

  @Test void extract_noSamplingDecision() {
    carrier.put("x-amzn-trace-id", sampledTraceId.replace("Sampled=1", "Sampled=?"));

    assertThat(extractor.extract(carrier).context())
        .isEqualTo(sampledContext.toBuilder().sampled(null).build());
  }

  @Test void extract_sampledFalse() {
    carrier.put("x-amzn-trace-id", sampledTraceId.replace("Sampled=1", "Sampled=0"));

    assertThat(extractor.extract(carrier).context())
        .isEqualTo(sampledContext.toBuilder().sampled(false).build());
  }

  /** Shows we skip whitespace and extra fields like self or custom ones */
  // https://aws.amazon.com/blogs/aws/application-performance-percentiles-and-request-tracing-for-aws-application-load-balancer/
  @Test void extract_skipsSelfField() {
    // TODO: check with AWS if it is valid to have arbitrary fields in front of standard ones.
    // we currently permit them
    carrier.put(
        "x-amzn-trace-id",
        "Robot=Hello;Self=1-582113d1-1e48b74b3603af8479078ed6;  "
            + "Root=1-58211399-36d228ad5d99923122bbe354;  "
            + "TotalTimeSoFar=112ms;CalledFrom=Foo");

    TraceContextOrSamplingFlags extracted = extractor.extract(carrier);
    assertThat(extracted.traceIdContext())
        .isEqualTo(
            TraceIdContext.newBuilder()
                .traceIdHigh(lowerHexToUnsignedLong("5821139936d228ad"))
                .traceId(lowerHexToUnsignedLong("5d99923122bbe354"))
                .build());

    assertThat(((AmznTraceId) extracted.extra().get(0)).customFields)
        .contains(new StringBuilder(";Robot=Hello;TotalTimeSoFar=112ms;CalledFrom=Foo"));
  }

  @Test void toString_fields() {
    AmznTraceId amznTraceId = new AmznTraceId(";Robot=Hello;TotalTimeSoFar=112ms;CalledFrom=Foo");

    assertThat(amznTraceId).hasToString(
        "AmznTraceId{customFields=" + amznTraceId.customFields + "}");
  }

  @Test void toString_none() {
    AmznTraceId amznTraceId = new AmznTraceId("");

    assertThat(amznTraceId).hasToString("AmznTraceId{}");
  }

  @Test void injectExtraStuff() {
    AmznTraceId amznTraceId = new AmznTraceId(";Robot=Hello;TotalTimeSoFar=112ms;CalledFrom=Foo");
    TraceContext extraContext =
        sampledContext.toBuilder().clearExtra().addExtra(amznTraceId).build();
    injector.inject(extraContext, carrier);

    assertThat(carrier)
        .containsEntry(
            "x-amzn-trace-id",
            "Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1;Robot=Hello;TotalTimeSoFar=112ms;CalledFrom=Foo");
  }

  @Test void extract_skipsLaterVersion() {
    carrier.put("x-amzn-trace-id", "Root=2-58211399-36d228ad5d99923122bbe354");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_skipsTruncatedId() {
    carrier.put("x-amzn-trace-id", "Root=1-58211399-36d228ad5d99923122bbe35");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_skips_leadingEquals() {
    carrier.put("x-amzn-trace-id", "=Root=1-58211399-36d228ad5d99923122bbe354");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_skips_doubleEquals() {
    carrier.put("x-amzn-trace-id", "Root==1-58211399-36d228ad5d99923122bbe354");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_skips_noEquals() {
    carrier.put("x-amzn-trace-id", "1-58211399-36d228ad5d99923122bbe354");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_skips_malformed() {
    carrier.put(
        "x-amzn-trace-id", "Sampled=-;Parent=463ac35%Af6413ad;Root=1-??-abc!#%0123456789123456");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_skips_really_malformed() {
    carrier.put("x-amzn-trace-id", "holy 💩");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_malformed_check_not_throws_exception_different_length() {
    carrier.put("x-amzn-trace-id", "Root=1-1373cbb-77f4b48ed7ff3eebbd62b5e01;Parent=1a4e96536a3ff131;Sampled=0");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }

  @Test void extract_malformed_throws_exception_not_hexadecimal_value() {
    carrier.put("x-amzn-trace-id", "Root=1-1z373cbb-77f4b48ed7ff3eebbd62b5e01;Parent=1a4e96536a3ff131;Sampled=0");

    assertThat(extractor.extract(carrier).samplingFlags()).isEqualTo(SamplingFlags.EMPTY);
  }
}
