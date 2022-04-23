package com.example.myapp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.example.aws.ec2.Ec2Operations;
import com.example.aws.s3.S3BucketOps;
import com.example.aws.sqs.MessageOperations;
import com.example.aws.sqs.QueueOperations;
import com.example.instances.Worker;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class App {

    public static void main(String[] args) throws IOException {

        Worker.analizeText("input.txt");
        System.exit(0);


        if (args.length == 0)
            System.exit(0);
        String[] myArgs2 = {"edu.stanford.nlp.parser.nndep.DependencyParser", "input.txt"};
//        DependencyParser.main(myArgs);
//        java -cp stanford-parser.jar:. -mx200m edu.stanford.nlp.parser.lexparser.LexicalizedParser -retainTMPSubcategories -outputFormat "wordsAndTags,penn,typedDependencies" englishPCFG.ser.gz input.txt
//        "-retainTMPSubcategories", "-outputFormat", "wordsAndTags,penn,typedDependencies",
        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder().region(region).build();

        String bucket = "dsp" + System.currentTimeMillis();
        String key = "input-file";

        S3BucketOps.createBucket(s3, bucket, region);
        System.out.println("Uploading Input file...");

        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromFile(new File(args[0])));

        System.out.println("Upload complete");
        System.out.printf("%n");

        String queueName = "queue" + System.currentTimeMillis();

        SqsClient sqs = SqsClient.builder()
                .region(region)
                .build();

        String queueURL = QueueOperations.createQueue(sqs, queueName);

        Map<String, MessageAttributeValue> messageAttributeValueMap = new HashMap<>();
        messageAttributeValueMap.put("n", MessageAttributeValue.builder().stringValue(args[3]).build());
        Message inputMessage = Message.builder().messageAttributes(messageAttributeValueMap).build();
        String messageBody = new HashMap<String, String>() {{
            put("bucket", bucket);
            put("key", key);
        }}.toString();
        MessageOperations.sendMessage(sqs, queueURL, messageBody, messageAttributeValueMap);

        Ec2Client ec2 = Ec2Client.builder().region(region).build();

        String managerInstanceId = Ec2Operations.getManagerInstance(ec2);
        Ec2Operations.startInstance(ec2, managerInstanceId);



//        cleanUp(s3, bucket, key);

        System.out.println("Closing the connection to {S3, Sqs, Ec2}");
        s3.close();
        sqs.close();
        ec2.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");
    }

    public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object: " + keyName);
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println(keyName + " has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }
}