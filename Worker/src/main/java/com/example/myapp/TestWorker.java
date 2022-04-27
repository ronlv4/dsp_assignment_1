package com.example.myapp;

import com.example.aws.ec2.Ec2Operations;
import com.example.aws.sqs.MessageOperations;
import com.example.aws.sqs.QueueOperations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.servicemetadata.IotanalyticsServiceMetadata;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class TestWorker {
    public static void testOnCloud() throws IOException {

        Region region = Region.US_WEST_2;

        Ec2Client ec2 = Ec2Client.builder().region(region).build();

        byte[] userDataScriptAsBytes = Files.readAllBytes(FileSystems.getDefault().getPath("./managerUserData"));
        String encodedUserDataScript = Base64.getEncoder().encodeToString(userDataScriptAsBytes);

        String instanceId = Ec2Operations.createEC2Instance(ec2, "Worker", "ami-02b92c281a4d3dc79", encodedUserDataScript);
        Ec2Operations.startInstance(ec2, instanceId);

    }

    public static void testLocally(){
        Region region = Region.US_WEST_2;


        SqsClient sqs = SqsClient.builder().region(region).build();

        String inputQueueUrl = QueueOperations.createQueue(sqs, "input-1");
        String outputQueueUrl = QueueOperations.createQueue(sqs, "output-1");
        Map<String, MessageAttributeValue> tempMap = new HashMap<>();
        tempMap.put("output-bucket", MessageAttributeValue.builder().stringValue("dspbucket12345").dataType("String").build());
        tempMap.put("analysis-type", MessageAttributeValue.builder().dataType("String").stringValue("CONSTITUENCY").build());
        tempMap.put("url", MessageAttributeValue.builder().dataType("String").stringValue("https://www.gutenberg.org/files/1659/1659-0.txt").build());
        MessageOperations.sendMessage(sqs, inputQueueUrl, "some non-empty message", tempMap);
        MessageOperations.sendMessage(sqs,inputQueueUrl, "terminate");
    }



    public static void main(String[] args) throws IOException {
        testLocally();
        testOnCloud();
//        Worker.main(new String[0]);
    }


}
