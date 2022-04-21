package com.dsp.instances;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Manager {

    public static String[] downloadInputFromS3(String url){
        return new String[2];
    }

    public static void main(String[] args) {
        String inputURL = args[0];
        String[] textFilesUrls = downloadInputFromS3(inputURL);

        SqsClient sqsClient = SqsClient.create();
//        String queueURL = SqsQueue.createQueue(sqsClient, "tasksQueue");

        for (String url :
                textFilesUrls) {
            sqsClient.sendMessage(SendMessageRequest.builder().build());
        }




    }
}
