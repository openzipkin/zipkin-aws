package brave.instrumentation.aws;

import brave.ErrorParser;
import brave.ScopedSpan;
import brave.SpanCustomizer;
import brave.Tracing;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import java.util.List;

public class Main {
  public static void main(String[] args) {
    Tracing tracing = Tracing.newBuilder()
        .spanReporter(span -> System.out.println(span.toString()))
        .errorParser(new ErrorParser() {
          @Override protected void error(Throwable error, Object span) {
            annotate(span, error.getLocalizedMessage());
            tag(span,"error", error.getLocalizedMessage());
          }
        })
        .build();

    ScopedSpan scopedSpan = tracing.tracer().startScopedSpan("s3-test");

    final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    try {
      List<Bucket> buckets = s3.listBuckets();
      System.out.println("Your Amazon S3 buckets are:");
      for (Bucket b : buckets) {
        System.out.println("* " + b.getName());
      }
    } catch (Exception e) {
    }

    try {
      s3.createBucket("test");
    } catch (AmazonS3Exception e) {
      System.err.println(e.getErrorMessage());
    }

    scopedSpan.finish();
  }
}
