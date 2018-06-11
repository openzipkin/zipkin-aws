/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.collector.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import zipkin2.Callback;
import zipkin2.collector.Collector;

final class KinesisSpanProcessor implements IRecordProcessor {
  static final Callback<Void> NOOP =
      new Callback<Void>() {
        @Override
        public void onSuccess(Void value) {}

        @Override
        public void onError(Throwable t) {}
      };

  private final Collector collector;

  KinesisSpanProcessor(Collector collector) {
    this.collector = collector;
  }

  @Override
  public void initialize(InitializationInput initializationInput) {}

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    for (Record record : processRecordsInput.getRecords()) {
      collector.acceptSpans(record.getData().array(), NOOP);
    }
  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {}
}
