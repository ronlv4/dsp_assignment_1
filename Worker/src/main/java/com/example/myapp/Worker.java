package com.example.myapp;

import java.io.*;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

import com.amazonaws.util.EC2MetadataUtils;
import com.example.aws.sqs.MessageOperations;
import com.example.aws.sqs.QueueOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Worker {

    public static final Region region = Region.US_EAST_1;
    static SqsClient sqs = SqsClient.builder().region(region).build();
    static S3Client s3 = S3Client.builder().region(region).build();
    static final Logger log = LogManager.getLogger();

    public static int execute(String inputQueueUrl, String[] args) {

        while (true) {

            log.info("receiving messages from {}", inputQueueUrl);
            ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(ReceiveMessageRequest
                    .builder()
                    .waitTimeSeconds(20)
                    .queueUrl(inputQueueUrl)
                    .maxNumberOfMessages(1)
                    .messageAttributeNames("fileUrl", "analysis", "bucket", "responseQueueName", "order")
                    .build());

            if (!receiveMessageResponse.hasMessages())
                continue;

            Message message = receiveMessageResponse.messages().get(0);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        System.out.println("Changing message visibility to 15 minutes");
                        MessageOperations.changeMessageVisibility(sqs, inputQueueUrl, message, ((int) TimeUnit.MINUTES.toSeconds(15)));
                    } catch (Exception e) {
                        log.error("Could not change message visibility", e);
                    }
                }
            }, 50, TimeUnit.MINUTES.toMillis(10));

            if (message.body().equals("terminate")) {
                log.info("got terminate message");
                timer.cancel();
                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);
                return 0;
            }

            String outputBucket = message.messageAttributes().get("bucket").stringValue();
            String outputQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest
                            .builder()
                            .queueName(message.messageAttributes().get("responseQueueName").stringValue())
                            .build())
                    .queueUrl();

            try {
                File outputFile = new WorkerExecution(message).call();

                s3.putObject(PutObjectRequest.builder()
                                .bucket(outputBucket)
                                .key(message.messageId())
                                .build(),
                        RequestBody.fromFile(outputFile));

                outputFile.delete();

                String outputUrl = s3.utilities().getUrl(GetUrlRequest
                        .builder()
                        .bucket(outputBucket)
                        .key(message.messageId())
                        .build()).toString();

                MessageOperations.sendMessage(sqs, outputQueueUrl, "Success",
                        new HashMap<String, MessageAttributeValue>() {{
                            put("outputUrl", MessageAttributeValue.builder().dataType("String").stringValue(outputUrl).build());
                            put("inputUrl", message.messageAttributes().get("fileUrl"));
                            put("analysis", message.messageAttributes().get("analysis"));
                            put("order", message.messageAttributes().get("order"));
                        }});
                timer.cancel();
                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);
            } catch (Exception e) {
                timer.cancel();
                MessageOperations.changeMessageVisibility(sqs, inputQueueUrl, message, ((int) TimeUnit.MINUTES.toSeconds(1)));
                MessageOperations.sendMessage(sqs, outputQueueUrl, "Error: " + e.getMessage());
                return 1;
            }
        }
    }

    public static void main(String[] args) {

        String failedWorkerQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName("failedWorker").build()).queueUrl();

        log.info("requesting queue url of WorkerQueue");
        String inputQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest
                .builder()
                .queueName("WorkerQueue")
                .build()).queueUrl();
        log.info("received");

        int finishedCode = 1;
        while (finishedCode == 1) {
            try {
                finishedCode = execute(inputQueueUrl, args);
            } catch (Exception e) {

                sqs.sendMessage(SendMessageRequest
                        .builder()
                        .queueUrl(failedWorkerQueueUrl)
                        .messageBody("Error: Unhandled exception occurred" + e.getMessage())
                        .build());
            }
        }
        close();
    }

    private static void close() {
        s3.close();
        sqs.close();
        Ec2Client.builder().build().terminateInstances(TerminateInstancesRequest.builder().instanceIds(EC2MetadataUtils.getInstanceId()).build());
    }
}