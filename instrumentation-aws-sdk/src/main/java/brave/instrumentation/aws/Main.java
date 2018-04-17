package brave.instrumentation.aws;

import brave.Tracing;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import java.util.List;

public class Main {
  public static void main(String[] args) {
    Tracing tracing = Tracing.newBuilder()
        .spanReporter(span -> System.out.println(span.toString()))
        .build();

    final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    List<Bucket> buckets = s3.listBuckets();
    System.out.println("Your Amazon S3 buckets are:");
    for (Bucket b : buckets) {
      System.out.println("* " + b.getName());
    }
  }
}
