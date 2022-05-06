package com.example.myapp;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.example.aws.sqs.MessageOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Worker {

    static final Logger log = LogManager.getLogger();

    public static void execute(String[] args, SqsClient sqs, S3Client s3){
        boolean shouldTerminate = false;

        log.info("requesting queue url of WorkerQueue");
        String inputQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest
                .builder()
                .queueName("WorkerQueue")
                .build()).queueUrl();
        log.info("received");

        while (!shouldTerminate) {

            log.info("receiving messages from {}", inputQueueUrl);
            ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(ReceiveMessageRequest
                    .builder()
                    .waitTimeSeconds(20)
                    .queueUrl(inputQueueUrl)
                    .messageAttributeNames("fileUrl", "analysis", "bucket", "responseQueue")
                    .build());

            if (!receiveMessageResponse.hasMessages())
                continue;

            Message message = receiveMessageResponse.messages().get(0);
            if (message.body().equals("terminate")) {
                log.info("got terminate message");
                shouldTerminate = true;
                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);
                continue;
            }

            String outputBucket = message.messageAttributes().get("bucket").stringValue();
            String outputQueueUrl = message.messageAttributes().get("responseQueue").stringValue();

            ExecutorService parsingExecution = Executors.newSingleThreadExecutor();
            Future<File> outputFile;

            try{
                outputFile = parsingExecution.submit(new WorkerExecution(message, s3, sqs));
                while (!outputFile.isDone()) {
                    log.info("Changing message visibility to 15 minutes");
                    MessageOperations.changeMessageVisibility(sqs, inputQueueUrl, message, ((int) TimeUnit.MINUTES.toSeconds(15)));
                    outputFile.get(10, TimeUnit.MINUTES);
                }
                parsingExecution.shutdown();

                s3.putObject(PutObjectRequest.builder()
                                .bucket(outputBucket)
                                .key(message.messageId())
                                .build(),
                        RequestBody.fromFile(outputFile.get()));

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
                        }});

                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);

            } catch (Exception e) {
                MessageOperations.changeMessageVisibility(sqs, inputQueueUrl, message, ((int) TimeUnit.MINUTES.toSeconds(1)));
                MessageOperations.sendMessage(sqs, outputQueueUrl, "Error: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;

        SqsClient sqs = SqsClient
                .builder()
                .region(region)
                .build();

        S3Client s3 = S3Client
                .builder()
                .region(region)
                .build();

        String failedWorkerQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName("failedWorker").build()).queueUrl();

        try {
            execute(args, sqs, s3);
        } catch (Exception e){

            sqs.sendMessage(SendMessageRequest
                    .builder()
                    .queueUrl(failedWorkerQueueUrl)
                    .messageBody("Error: Unhandled exception occurred")
                    .build());
        }
        finally {
            s3.close();
            sqs.close();
            Ec2Client.builder().build().terminateInstances(TerminateInstancesRequest.builder().instanceIds(EC2MetadataUtils.getInstanceId()).build());
        }




    }
}