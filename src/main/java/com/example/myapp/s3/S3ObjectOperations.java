package com.example.myapp.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class S3ObjectOperations {

    public static void emptyBucket(S3Client s3, String bucket) {
        ListObjectsResponse listObjectsResponse = s3.listObjects(ListObjectsRequest.builder()
                .bucket(bucket).build());
        listObjectsResponse.contents().forEach(s3Object -> s3.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(s3Object.key())
                .build()));
    }
}
