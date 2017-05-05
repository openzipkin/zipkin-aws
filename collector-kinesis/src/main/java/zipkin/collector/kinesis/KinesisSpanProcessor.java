/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin.collector.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import java.util.ArrayList;
import java.util.List;
import zipkin.Codec;
import zipkin.collector.Collector;
import zipkin.internal.Nullable;
import zipkin.storage.Callback;

public class KinesisSpanProcessor implements IRecordProcessor {

  private final Collector collector;

  KinesisSpanProcessor(Collector collector) {
    this.collector = collector;
  }

  @Override
  public void initialize(InitializationInput initializationInput) {
  }

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    List<byte[]> spans =  new ArrayList<>();
    for (Record record : processRecordsInput.getRecords()) {
      spans.add(record.getData().array());
    }

    collector.acceptSpans(spans, Codec.THRIFT, new Callback<Void>() {
      @Override
      public void onSuccess(@Nullable Void aVoid) {
      }

      @Override
      public void onError(Throwable throwable) {
      }
    });
  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {
  }
}
