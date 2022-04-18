package com.example.myapp;

import java.io.File;
import java.io.IOException;

import com.example.myapp.s3.S3BucketOps;
import com.example.myapp.sqs.MessageOperations;
import com.example.myapp.sqs.QueueOperations;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import com.example.dsp.Models.SqsInputMessage;


public class App {

    public static void main(String[] args) throws IOException {

        if (args.length == 0)
            System.exit(0);
        String[] myArgs = {"-retainTMPSubcategories", "-outputFormat", "wordsAndTags,penn,typedDependencies", "englishPCFG.ser.gz", "input.txt"};
        String[] myArgs2 = {"-retainTMPSubcategories -outputFormat \"wordsAndTags,penn,typedDependencies\" englishPCFG.ser.gz input.txt"};
        LexicalizedParser.main(myArgs);
        System.exit(0);
//        java -cp stanford-parser.jar:. -mx200m edu.stanford.nlp.parser.lexparser.LexicalizedParser -retainTMPSubcategories -outputFormat "wordsAndTags,penn,typedDependencies" englishPCFG.ser.gz input.txt

        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder().region(region).build();

        String bucket = "dsp" + System.currentTimeMillis();
        String key = "input-file";

        S3BucketOps.createBucket(s3, bucket, region);
        System.out.println("Uploading object...");

        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromFile(new File(args[0])));

        System.out.println("Upload complete");
        System.out.printf("%n");

        String queueName = "queue" + System.currentTimeMillis();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        String queueURL = QueueOperations.createQueue(sqsClient, queueName);

        MessageOperations.sendMessage(sqsClient, queueURL, new SqsInputMessage(bucket, key).serializeMessage());

        sqsClient.close();

        // start manager if exists else create one and start

//        cleanUp(s3, bucket, key);

        System.out.println("Closing the connection to {S3}");
        s3.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");
    }

    public static void createBucket(S3Client s3Client, String bucketName, Region region) {
        try {
            System.out.println("Creating bucket: " + bucketName);
            s3Client.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(region.id())
                                    .build())
                    .build());
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println(bucketName +" is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object: " + keyName);
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println(keyName +" has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName +" has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }
}