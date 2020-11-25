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
package brave.instrumentation.aws.sqs;

import brave.Tracing;
import com.amazonaws.handlers.RequestHandler2;

public class SqsMessageTracing {
  public static SqsMessageTracing create(Tracing tracing) {
    return new SqsMessageTracing(tracing);
  }

  final RequestHandler2 requestHandler;

  private SqsMessageTracing(Tracing tracing) {
    this.requestHandler = new SendMessageTracingRequestHandler(tracing);
  }

  public RequestHandler2 requestHandler() {
    return requestHandler;
  }
}
