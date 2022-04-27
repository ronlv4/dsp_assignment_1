import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class S3Connector {

    private final S3Client s3;

    public S3Connector(Region region){
        this.s3 = S3Client.builder().region(region).build();
    }

    public void writeFileToS3(String bucket, String key, File file){
        if(!bucketExists(s3, bucket))
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(file));
    }

    public void writeStringToS3(String bucket, String key, String s){
        if(!bucketExists(s3, bucket))
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromString(s));
    }

    public void readFileFromS3(String bucket, String key, String fileName) {
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(), Path.of(fileName));
    }

    public String readStringFromS3(String bucket, String key){
        try {
            return new String(new ByteArrayInputStream(s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()).readAllBytes()).readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    public void deleteBucket(String bucket){
        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
    }

    public void deleteS3Object(String bucket, String key){
        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
    }

    private boolean bucketExists(S3Client s3, String bucket){
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucket)
                .build();

        try {
            s3.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }
}
