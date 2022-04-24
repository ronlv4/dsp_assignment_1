package com.example.myapp;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.example.aws.sqs.MessageOperations;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Worker {

    private static File processMessage(Message message) {
        String analyisType = String.valueOf(message.messageAttributes().get("analysis-type"));
        String fileUrl = String.valueOf(message.messageAttributes().get("url"));
        return analyzeText(fileUrl, analyisType);
    }

    public static File analyzeText(String url, String analysisType) {
        String outputFormat;
        String outputFileDirectory = ".";
        if (analysisType.equals("DEPENDENCY"))
            outputFormat = "typedDependencies";
        else if (analysisType.equals("CONSTITUENCY"))
            outputFormat = "penn";
        else
            outputFormat = "wordsAndTags";

        String[] parserArgs = {
                "-model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz",
                "-outputFormat", outputFormat,
                "-writeOutputFiles",
                "-outputFilesDirectory", outputFileDirectory,
//                "-outputFilesPrefix", "output4",  // currently on Test option - not production
//                "-retainTMPSubcategories",
//                "-outputFilesExtension", "txt",
                url};
        LexicalizedParser.main(parserArgs);
        return new File(outputFileDirectory);
    }

    public static void main(String[] args) {
        boolean shouldTerminate = false;
        Region region = Region.US_WEST_2;

        SqsClient sqs = SqsClient.builder()
                .region(region)
                .build();

        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder().queueName("input-1").build());
        sqs.createQueue(CreateQueueRequest.builder().queueName("output-1").build());
        Map<String, MessageAttributeValue> tempMap = new HashMap<>();
        tempMap.put("bucket", MessageAttributeValue.builder().stringValue("dspbucket12345").build());
        tempMap.put("analysis-type", MessageAttributeValue.builder().stringValue("CONSTITUENCY").build());
        tempMap.put("url", MessageAttributeValue.builder().stringValue("https://www.gutenberg.org/files/1659/1659-0.txt").build());
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(createQueueResponse.queueUrl()).messageAttributes(tempMap).build());


        ListQueuesResponse listQueuesResponse = sqs
                .listQueues(ListQueuesRequest
                        .builder()
                        .queueNamePrefix("input")
                        .build());

        while (!shouldTerminate) {
            for (String queueUrl : listQueuesResponse.queueUrls()) {

                ReceiveMessageResponse receiveMessageResponse = sqs
                        .receiveMessage(ReceiveMessageRequest
                                .builder()
                                .queueUrl(queueUrl)
                                .build());

                if (!receiveMessageResponse.hasMessages())
                    continue;

                Message message = receiveMessageResponse.messages().get(0);
                if (message.body().equals("terminate")) {
                    shouldTerminate = true;
                    break;
                }

                String outputQueueUrl = sqs
                        .listQueues(ListQueuesRequest
                                .builder()
                                .queueNamePrefix("output")
                                .maxResults(1)
                                .build())
                        .queueUrls()
                        .get(0);

                try {
                    File outputFile = processMessage(message);
                    String outputBucket = String.valueOf(message.messageAttributes().get("bucket"));
                    PutObjectResponse putObjectResponse = s3.putObject(PutObjectRequest.builder().
                                    bucket(outputBucket)
                                    .key(message.messageId())
                                    .build(),
                            RequestBody.fromFile(outputFile));
                    outputFile.delete();
                    Map<String, MessageAttributeValue> messageAttributeValueMap = new HashMap<String, MessageAttributeValue>() {{
                        put("original-url", message.messageAttributes().get("url"));
                        put("object-url", MessageAttributeValue.builder()
                                .stringValue(s3.utilities().getUrl(GetUrlRequest
                                        .builder()
                                        .bucket(outputBucket)
                                        .key(message.messageId())
                                        .build()).toString())
                                .build());
                        put("analysis-type", message.messageAttributes().get("analysis-type"));
                    }};

                    MessageOperations.sendMessage(sqs, outputQueueUrl, "Success", messageAttributeValueMap);
                } catch (Exception e) {
                    MessageOperations.sendMessage(sqs, outputQueueUrl, "Error: " + e.getMessage());
                }

                receiveMessageResponse.messages().remove(message);
                shouldTerminate = true;
            }
        }

        s3.close();
        sqs.close();
    }
}