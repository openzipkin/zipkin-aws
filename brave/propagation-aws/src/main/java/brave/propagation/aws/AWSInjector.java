/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.propagation.aws;

import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import brave.propagation.aws.AWSPropagation.AmznTraceId;

import static brave.propagation.aws.AWSPropagation.AMZN_TRACE_ID_NAME;
import static brave.propagation.aws.AWSPropagation.PARENT;
import static brave.propagation.aws.AWSPropagation.ROOT;
import static brave.propagation.aws.AWSPropagation.SAMPLED;
import static brave.propagation.aws.AWSPropagation.writeRoot;
import static brave.propagation.aws.HexCodec.writeHexLong;

final class AWSInjector<R> implements TraceContext.Injector<R> {
  final AWSPropagation propagation;
  final Setter<R, String> setter;

  AWSInjector(AWSPropagation propagation, Setter<R, String> setter) {
    this.propagation = propagation;
    this.setter = setter;
  }

  /**
   * This version of propagation contains at least 74 characters corresponding to identifiers and
   * the sampling bit. It will also include extra fields where present.
   *
   * <p>Ex 74 characters: {@code
   * Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1}
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void inject(TraceContext traceContext, R request) {
    AmznTraceId amznTraceId = traceContext.findExtra(AmznTraceId.class);
    int customFieldsLength = amznTraceId != null ? amznTraceId.customFields.length() : 0;

    // Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1
    char[] result = new char[74 + customFieldsLength];
    System.arraycopy(ROOT, 0, result, 0, 5);
    writeRoot(traceContext, result, 5);
    System.arraycopy(PARENT, 0, result, 40, 8);
    writeHexLong(result, 48, traceContext.spanId());
    System.arraycopy(SAMPLED, 0, result, 64, 9);
    Boolean sampled = traceContext.sampled();

    // Sampled status is same as B3, but ? means downstream decides (like omitting X-B3-Sampled)
    // https://github.com/aws/aws-xray-sdk-go/blob/391885218b556c43ed05a1e736a766d70fc416f1/header/header.go#L50
    result[73] = sampled == null ? '?' : sampled ? '1' : '0';
    for (int i = 0; i < customFieldsLength; i++) {
      result[i + 74] = amznTraceId.customFields.charAt(i);
    }

    setter.put(request, AMZN_TRACE_ID_NAME, new String(result));
  }
}