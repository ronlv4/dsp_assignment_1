import Models.SqsMessage;
import Utils.Network;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.File;

public class Worker {

    public enum AnalysisType {
        POS,
        CONSTITUENCY,
        DEPENDENCY
    }

    private static Object[] parseMessage(String message){
        return new Object[2]; // #TODO
    }

    public static void main(String[] args) {
        SqsClient sqsClient = SqsClient.create();
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
            UploadPartResponse uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder().bucket("enter-bucket-here").build());
        }
    }



}
