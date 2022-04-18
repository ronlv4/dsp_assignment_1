package com.example.dsp;

import com.example.dsp.Utils.Network;
import com.example.myapp.sqs.MessageOperations;
import edu.stanford.nlp.parser.server.LexicalizedParserClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.Properties;

public class Worker {

    public enum AnalysisType {
        POS,
        CONSTITUENCY,
        DEPENDENCY
    }

    private static Object[] parseMessage(String message){
        return new Object[2]; // #TODO
    }

    private static void analizeText(File textFile){
        Properties props = new Properties();

    }

    public static void main(String[] args) {
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_WEST_2)
                .build();

        ListQueuesResponse listQueuesResponse = sqsClient.listQueues(ListQueuesRequest.builder().queueNamePrefix("input").build());
        for (String queueUrl: listQueuesResponse.queueUrls()){
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build());
            receiveMessageResponse.messages().forEach(message -> message.messageAttributes().get("analysis-type"));
        }
//        LexicalizedParserClient client = new LexicalizedParserClient()

//        MessageOperations.receiveMessages(sqsClient, )

        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().build());
        ReceiveMessageResponse messageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl("enter-url-here").build());
        for (Message message: messageResponse.messages()){
            String messageBody = message.body();
            Object[] messageArgs = parseMessage(messageBody);
            AnalysisType type = ((AnalysisType) messageArgs[0]);
            String url = ((String) messageArgs[1]);
            File textFile = Network.downloadFile(url);
            analizeText(textFile);
            S3Client s3Client = S3Client.create();
//            UploadPartResponse uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket("enter-bucket-here").build());
        }
    }



}
