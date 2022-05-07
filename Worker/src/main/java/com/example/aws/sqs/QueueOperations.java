package com.example.aws.sqs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class QueueOperations {
    static final Logger log = LogManager.getLogger();

    public static String createQueue(SqsClient sqsClient, String queueName) {
        try {
            log.info("Create Queue");

            sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build());

            log.info("Get queue url");

            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            log.info("createdURL: {}", queueUrl);
            return queueUrl;

        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    public static void listQueues(SqsClient sqsClient) {

        log.info("List Queues");
        String prefix = "que";

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

            for (String url : listQueuesResponse.queueUrls()) {
                System.out.println(url);
            }

        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }

    public static void listQueuesFilter(SqsClient sqsClient, String queueUrl, String namePrefix) {
        // List queues with filters
        ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                .queueNamePrefix(namePrefix).build();

        ListQueuesResponse listQueuesFilteredResponse = sqsClient.listQueues(filterListRequest);
        log.info("Queue URLs with prefix: " + namePrefix);
        for (String url : listQueuesFilteredResponse.queueUrls()) {
            System.out.println(url);
        }

        log.info("Send message");

        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("Hello world!")
                    .delaySeconds(10)
                    .build());

        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }
}