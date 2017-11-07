package zipkin2.storage.xray_udp;

import com.squareup.moshi.JsonWriter;
import java.io.IOException;
import okio.Buffer;

public final class XRayFormatter {

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
