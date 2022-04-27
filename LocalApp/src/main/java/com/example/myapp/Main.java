package com.example.myapp;

import com.example.aws.ec2.Ec2Operations;
import com.example.aws.s3.S3BucketOps;
import com.example.aws.sqs.MessageOperations;
import com.example.aws.sqs.QueueOperations;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class Main {

//    public static void main(String[] args){
//        s3Connector.writeFileToS3(bucket, key, inputFileName);
//        sqsConnector.sendMessage("shir", "file is in the queue");
//
//        Message m = null;
//        while(Objects.isNull(m)) {
//            m = sqsConnector.getMessage("shir1");
//            System.out.println(System.currentTimeMillis());
//        }
//        System.out.println(m.body());
//    }

    public static void main(String[] args) throws IOException {

        if (args.length < 3)
            throw new IllegalArgumentException("Not enough arguments");
        String inputFileName = args[0];
        String outputFileName = args[1];
        Integer n = Integer.parseInt(args[2]);
        boolean terminate = args.length > 3;

        byte[] userDataScriptAsBytes = Files.readAllBytes(FileSystems.getDefault().getPath("LocalApp/localAppUserData"));
        String encodedUserDataScript = Base64.getEncoder().encodeToString(userDataScriptAsBytes);

        System.out.println(encodedUserDataScript);
//        if (args.length == 0)
//            System.exit(0);
        Region region = Region.US_WEST_2;
        Ec2Client ec2Client = Ec2Client.builder().region(region).build();
        Ec2Operations.createEC2Instance(ec2Client, "Test", "ami-02b92c281a4d3dc79", encodedUserDataScript);
        System.exit(0);

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

        System.out.println("Closing the connection to {S3, Sqs, Ec2}");
        s3.close();
        sqs.close();
        ec2.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");
    }
}
