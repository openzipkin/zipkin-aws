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
package brave.instrumentation.aws.sqs.propogation;

import brave.Span;
import brave.Tracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import static brave.Span.Kind.PRODUCER;

/**
 * Used to consume the tracing information from the message attributes of the SQS message.
 *
 * @see SendMessageRemoteSetter for placing this information into the message attributes
 */
public class SendMessageRemoteGetter
    implements Propagation.RemoteGetter<Map<String, MessageAttributeValue>> {

  @Override
  public Span.Kind spanKind() {
    return PRODUCER;
  }

  @Override
  public String get(final Map<String, MessageAttributeValue> request, final String fieldName) {
    return Optional.ofNullable(request.get(fieldName))
        .map(MessageAttributeValue::stringValue)
        .orElse(null);
  }

  /**
   * Helper static function to create an extractor for this {@link Propagation.RemoteGetter}.
   *
   * @param tracing trace instrumentation utilities
   * @return the extractor
   */
  public static TraceContext.Extractor<Map<String, MessageAttributeValue>> create(
      final Tracing tracing) {
    return tracing.propagation().extractor(new SendMessageRemoteGetter());
  }
}
