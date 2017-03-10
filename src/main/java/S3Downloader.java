import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;

/**
 * A Simple S3 (S4?) downloader which streams straight to stdout
 * Created by pallav.kothari on 3/10/17.
 */
public class S3Downloader {
    public static void main(String[] args) throws IOException, InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();

        Preconditions.checkNotNull(System.getenv("AWS_ACCESS_KEY_ID"), "missing aws key; do `export AWS_ACCESS_KEY_ID=<your_access_key>");
        Preconditions.checkNotNull(System.getenv("AWS_SECRET_ACCESS_KEY"), "missing aws key; do `export AWS_SECRET_ACCESS_KEY=<your_secret_key>");
        Preconditions.checkNotNull(System.getenv("AWS_REGION"), "missing AWS_REGION; do `export AWS_REGION=<your_region>");
        Preconditions.checkArgument(args.length == 2, "expected 2 arguments: bucket_name, file_name");

        String bucketName = args[0];
        String key = args[1];

        new S3Downloader().downloadWithClient(bucketName, key);

        System.out.println("Done. Took " + stopwatch);
    }

    private void downloadWithClient(String bucket, String key) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        S3Object s3Object = s3Client.getObject(bucket, key);

        try (InputStream content = s3Object.getObjectContent()) {
//            ByteSink byteSink = Files.asByteSink(output);
            ByteStreams.copy(content, System.out);
        }
    }


//    File output = new File(key);
//        output.createNewFile();
//        output.deleteOnExit();
//        TransferManager transferManager = TransferManagerBuilder.defaultTransferManager();
//        Download download = transferManager.download(getObjectRequest, output);
//        while (!download.isDone()) {
//            Thread.sleep(5000);
//            System.out.println(String.format("%s downloading.. (%,.2f%%)", key, download.getProgress().getPercentTransferred()));
//        }
//        download.waitForCompletion();
//        transferManager.shutdownNow();

}
