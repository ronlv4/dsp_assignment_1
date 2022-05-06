package com.example.aws.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

public class S3BucketOps {
    static final Logger log = LogManager.getLogger();

    public static void createBucket(S3Client s3Client, String bucketName, Region region) {
        log.info("creating bucket with name {} in region {}", bucketName, region);

        try {
            S3Waiter s3Waiter = s3Client.waiter();
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(region.id())
                                    .build())
                    .build());
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();


            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            log.info(bucketName + " is ready");

        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }
    // snippet-end:[s3.java2.s3_bucket_ops.create_bucket]

    public static void deleteNonEmptyBuckets(S3Client s3){
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
        for (Bucket bucket : listBucketsResponse.buckets()) {
            S3ObjectOperations.emptyBucket(s3, bucket.name());
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket.name()).build());
        }
    }
}
