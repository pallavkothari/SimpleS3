import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import java.io.File;

/**
 * Upload a file to an s3 bucket
 *
 * Created by pallav.kothari on 3/10/17.
 */
public class S3Uploader {
    public static void main(String[] args) throws InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();

        Preconditions.checkNotNull(System.getenv("AWS_ACCESS_KEY_ID"), "missing aws key; do `export AWS_ACCESS_KEY_ID=<your_access_key>");
        Preconditions.checkNotNull(System.getenv("AWS_SECRET_ACCESS_KEY"), "missing aws key; do `export AWS_SECRET_ACCESS_KEY=<your_secret_key>");
        Preconditions.checkNotNull(System.getenv("AWS_REGION"), "missing AWS_REGION; do `export AWS_REGION=<your_region>");
        Preconditions.checkArgument(args.length == 2, "expected 2 arguments: bucket_name, path_to_file");

        String bucketName = args[0];
        String file = args[1];
        Preconditions.checkArgument(new File(file).exists(), "invalid file " + file);

        new S3Uploader().upload(bucketName, file);

        System.out.println("Done. Took " + stopwatch);
    }

    private void upload(String bucketName, String file) throws InterruptedException {
        TransferManager transferManager = TransferManagerBuilder.defaultTransferManager();
        Upload upload = transferManager.upload(bucketName, file, new File(file));
        while (!upload.isDone()) {
            Thread.sleep(1000);
            System.out.println(String.format("%s uploading.. (%,.2f%%)", file, upload.getProgress().getPercentTransferred()));
        }
        upload.waitForCompletion();
        transferManager.shutdownNow();
    }
}
