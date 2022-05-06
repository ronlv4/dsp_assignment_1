package com.example.myapp;

import com.example.aws.ec2.Ec2Operations;
import com.example.aws.sqs.MessageOperations;
import com.example.aws.sqs.QueueOperations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class TestWorker {
    public static void testOnCloud() throws IOException {

        Region region = Region.US_EAST_1;

        Ec2Client ec2 = Ec2Client.builder().region(region).build();

        byte[] userDataScriptAsBytes = Files.readAllBytes(FileSystems.getDefault().getPath("./test-managerUserData"));
        String encodedUserDataScript = Base64.getEncoder().encodeToString(userDataScriptAsBytes);

        String instanceId = Ec2Operations.createEC2Instance(ec2, "Worker", "ami-0022f774911c1d690", encodedUserDataScript);
        Ec2Operations.startInstance(ec2, instanceId);

    }

    public static void createTestScene(){
        Region region = Region.US_EAST_1;


        SqsClient sqs = SqsClient.builder().region(region).build();

        String inputQueueUrl = QueueOperations.createQueue(sqs, "WorkerQueue");
        String outputQueueUrl = QueueOperations.createQueue(sqs, "responseQueue");
        String failedWorkerQueueUrl = QueueOperations.createQueue(sqs, "failedWorker");
        Map<String, MessageAttributeValue> tempMap = new HashMap<>();
        tempMap.put("bucket", MessageAttributeValue.builder().dataType("String").stringValue("dspassignment1").build());
        tempMap.put("analysis", MessageAttributeValue.builder().dataType("String").stringValue("DEPENDENCY").build());
        tempMap.put("responseQueue", MessageAttributeValue.builder().dataType("String").stringValue(outputQueueUrl).build());
        tempMap.put("fileUrl", MessageAttributeValue.builder().dataType("String").stringValue("https://www.gutenberg.org/files/1659/1659-0.txt").build());
        MessageOperations.sendMessage(sqs, inputQueueUrl, "some non-empty message", tempMap);
        MessageOperations.sendMessage(sqs,inputQueueUrl, "terminate");
    }

    public static void testLocally(){
        Worker.main(new String[0]);
    }



    public static void main(String[] args) throws IOException {
        createTestScene();
        testOnCloud();
//        testLocally();
    }


}
