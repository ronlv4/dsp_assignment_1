package com.example.myapp;

import com.example.aws.ec2.Ec2Operations;
import com.example.aws.sqs.MessageOperations;
import com.example.aws.sqs.QueueOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TestWorker {
    public static void testOnCloud() throws IOException {

        Region region = Region.US_EAST_1;

        Ec2Client ec2 = Ec2Client.builder().region(region).build();

        byte[] userDataScriptAsBytes = Files.readAllBytes(FileSystems.getDefault().getPath("./test-managerUserData"));
        String encodedUserDataScript = Base64.getEncoder().encodeToString(userDataScriptAsBytes);
        for (int i = 0; i < 3; i++) {
            String instanceId = Ec2Operations.createEC2Instance(ec2, "Worker", "ami-0022f774911c1d690", encodedUserDataScript);
            Ec2Operations.startInstance(ec2, instanceId);
        }

    }

    public static void createTestScene() {
        String file = "POS\thttps://www.gutenberg.org/files/1659/1659-0.txt\n" +
                "CONSTITUENCY\thttps://www.gutenberg.org/files/1659/1659-0.txt\n" +
                "DEPENDENCY\thttps://www.gutenberg.org/files/1659/1659-0.txt\n" +
                "POS\thttps://www.gutenberg.org/files/1660/1660-0.txt\n" +
                "CONSTITUENCY\thttps://www.gutenberg.org/files/1660/1660-0.txt\n" +
                "DEPENDENCY\thttps://www.gutenberg.org/files/1660/1660-0.txt\n" +
                "POS\thttps://www.gutenberg.org/files/1661/1661-0.txt\n" +
                "CONSTITUENCY\thttps://www.gutenberg.org/files/1661/1661-0.txt\n" +
                "DEPENDENCY\thttps://www.gutenberg.org/files/1661/1661-0.txt";
        Random random = new Random();
        Region region = Region.US_EAST_1;
        SqsClient sqs = SqsClient.builder().region(region).build();
        String inputQueueUrl = QueueOperations.createQueue(sqs, "WorkerQueue");
        String outputQueueUrl = QueueOperations.createQueue(sqs, "responseQueue");
        String failedWorkerQueueUrl = QueueOperations.createQueue(sqs, "failedWorker");
        String[] lines = file.split("\\r?\\n");
        Arrays.stream(lines).parallel().forEach(line -> {
            String analysis = line.split("\t")[0];
            String fileUrl = line.split("\t")[1];
            MessageOperations.sendMessage(sqs, inputQueueUrl, "some non-empty message", new HashMap<String, MessageAttributeValue>() {{
                put("bucket", MessageAttributeValue.builder().dataType("String").stringValue("dspassignment1").build());
                put("analysis", MessageAttributeValue.builder().dataType("String").stringValue(analysis).build());
                put("responseQueue", MessageAttributeValue.builder().dataType("String").stringValue(outputQueueUrl).build());
                put("fileUrl", MessageAttributeValue.builder().dataType("String").stringValue(fileUrl).build());
                put("order", MessageAttributeValue.builder().stringValue(Integer.toString(random.nextInt(1000))).dataType("String").build());
            }});
        });
        MessageOperations.sendMessage(sqs, inputQueueUrl, "terminate", ((int) TimeUnit.MINUTES.toSeconds(10)), new HashMap<>());
    }

    public static void testLocally() {
        Worker.main(new String[0]);
    }


    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getRootLogger();
        log.info("hello");
        createTestScene();
        testOnCloud();
//        testLocally();
    }


}
