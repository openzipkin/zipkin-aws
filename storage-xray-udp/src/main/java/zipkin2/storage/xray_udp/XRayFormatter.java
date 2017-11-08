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
package zipkin2.storage.xray_udp;

import com.squareup.moshi.JsonWriter;
import java.io.IOException;
import okio.Buffer;

/**
 * This class is an utility class providing some basic ways to create simple JSON's.
 * Since a span can be annotated by only a String value, passing input as JSON is required.
 */
public final class XRayFormatter {

  /**
   *
   * Formating an exception to be consumed by XRay by Spans annotations
   *
   * @param exceptionId an unique exception id @see brave.internal.Platform#randomLong()
   * @param isRemote Any calls to a remote service/broker should be true
   * @param maxStackTraceElement XRay has a limit of 60KB per segment. Choose a wise number
   * @param throwable The exception occured
   * @return a JSON String which is ready to be consumed by XRay "cause.exception"
   * @throws IOException
   */
  public static String formatCause(String exceptionId, boolean isRemote, int maxStackTraceElement,
                                   Throwable throwable) throws IOException {
    Buffer buffer = new Buffer();
    JsonWriter writer = JsonWriter.of(buffer);
    writer.beginArray();
    writer.beginObject();
    writer.name("id").value(exceptionId);
    writer.name("type").value(throwable.getClass().getName());
    writer.name("remote").value(isRemote);
    writer.name("stack").beginArray();
    StackTraceElement[] stackTrace = throwable.getStackTrace();
    for (int i = 0; i < stackTrace.length; i++) {
      StackTraceElement aTrace = stackTrace[i];
      writer.beginObject();
      writer.name("path").value(aTrace.getFileName());
      writer.name("line").value(aTrace.getLineNumber());
      writer.name("label").value(aTrace.getClassName() + "." + aTrace.getMethodName());
      writer.endObject();
      if(maxStackTraceElement < i) break;
    }
    writer.endArray();
    writer.name("truncated").value(stackTrace.length > maxStackTraceElement);
    writer.endObject();
    writer.endArray();
    return buffer.readByteString().utf8();
  }

}
