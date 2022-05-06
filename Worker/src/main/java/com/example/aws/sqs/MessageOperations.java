package com.example.aws.sqs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;

public class MessageOperations {
    static final Logger log = LogManager.getLogger();
    public static void sendMessage(SqsClient sqsClient, SendMessageRequest messageRequest) {
        sqsClient.sendMessage(messageRequest);
    }

    public static void sendMessage(SqsClient sqsClient, String queueUrl, String messageBody) {
        sendMessage(sqsClient, queueUrl, messageBody, null);
    }

    public static void sendMessage(SqsClient sqsClient, String queueUrl, String messageBody, Map<String, MessageAttributeValue> messageAttributeValueMap) {
        sendMessage(sqsClient, SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributes(messageAttributeValueMap)
                .messageBody(messageBody)
                .delaySeconds(10)
                .build());
    }

    public static void sendMessage(SqsClient sqsClient, String queueUrl, Map<String, MessageAttributeValue> messageAttributeValueMap) {
        sendMessage(sqsClient, SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributes(messageAttributeValueMap)
                .delaySeconds(10)
                .build());
    }


    public static void sendBatchMessages(SqsClient sqsClient, String queueUrl) {

        log.info("\nSend multiple messages to url: {}", queueUrl);

        try {
            // snippet-start:[sqs.java2.sqs_example.send__multiple_messages]
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody("Hello from msg 1").build(),
                            SendMessageBatchRequestEntry.builder().id("id2").messageBody("msg 2").delaySeconds(10).build())
                    .build();
            sqsClient.sendMessageBatch(sendMessageBatchRequest);
            // snippet-end:[sqs.java2.sqs_example.send__multiple_messages]

        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }

    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {

        log.info("\nReceive messages from queue: {}", queueUrl);

        try {
            // snippet-start:[sqs.java2.sqs_example.retrieve_messages]
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(5)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
        return null;
        // snippet-end:[sqs.java2.sqs_example.retrieve_messages]
    }

    public static void changeMessages(SqsClient sqs, String queueUrl, List<Message> messages, Integer visibilityTimeout) {

        log.info("\nChange Message Visibility to {}", visibilityTimeout);

        try {
            for (Message message : messages)
                changeMessageVisibility(sqs, queueUrl, message, visibilityTimeout);
        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }

    public static void deleteMessage(SqsClient sqsClient, String queueUrl, Message message) {
        log.info("\nDelete Message {}", message.messageId());

        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }


    public static void deleteMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {
        log.info("\nDelete Messages in queue {}", queueUrl);
        // snippet-start:[sqs.java2.sqs_example.delete_message]

        for (Message message : messages) {
            deleteMessage(sqsClient, queueUrl, message);
        }
        // snippet-end:[sqs.java2.sqs_example.delete_message]
    }

    public static ChangeMessageVisibilityResponse changeMessageVisibility(SqsClient sqs, String queueUrl, Message message, Integer visibilityTimeout) {
        log.info("\nChange message Visibility to {} seconds", visibilityTimeout);
        try {
            return sqs.changeMessageVisibility(ChangeMessageVisibilityRequest
                    .builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .visibilityTimeout(visibilityTimeout)
                    .build());
        } catch (SqsException e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
        return null;
    }


}
