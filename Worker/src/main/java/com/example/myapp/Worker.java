package com.example.myapp;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.LinkedList;
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
    public static String donwloadFile(URL url) throws IOException {
        return donwloadFile(url, "text" + System.currentTimeMillis());
    }

    public static String donwloadFile(URL url, String outputFilePath) throws IOException {
        ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath);
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel()
                .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        return outputFilePath;

    }

    private static File processMessage(Message message) throws IOException {
        System.out.println("Starting to process message\n");
        String analyisType = message.messageAttributes().get("analysis-type").stringValue();
        String fileUrl = message.messageAttributes().get("url").stringValue();
        System.out.println("Downloading text file\n");
        String filePath = donwloadFile(new URL(fileUrl));
        System.out.printf("Downloaded file path is %s\n", filePath);
//        System.out.printf("analyzing file from url %s as %s\n", fileUrl, analyisType);
        System.out.printf("running from %s\n", FileSystems.getDefault().getPath(".").toAbsolutePath());
        return analyzeText(filePath, analyisType);
    }

    public static File analyzeText(String filePath, String analysisType) {
        String outputFormat;
        String outputFileDirectory = ".";
        String outputFileExtension = "txt";
        if (analysisType.equals("DEPENDENCY"))
            outputFormat = "typedDependencies";
        else if (analysisType.equals("CONSTITUENCY"))
            outputFormat = "penn";
        else
            outputFormat = "wordsAndTags";

        String[] parserArgs = {
                "-model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz",
                "-sentences", "newline",
//                "-maxLength", "80",
                "-outputFormat", outputFormat,
                "-writeOutputFiles",
                "-outputFilesExtension", outputFileExtension,
//                "-outputFilesDirectory", outputFileDirectory,
//                "-outputFilesPrefix", "output4",  // currently on Test option - not production
//                "-retainTMPSubcategories",
                filePath};
        LexicalizedParser.main(parserArgs);
        System.out.println("\nfinished analyzing\n");
        deleteFile(new File(filePath));
        return new File(outputFileDirectory + "/" + filePath + "." + outputFileExtension);
    }

    private static boolean deleteFile(File file) {
        return file.delete();
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


        ListQueuesResponse listQueuesResponse = sqs
                .listQueues(ListQueuesRequest
                        .builder()
                        .queueNamePrefix("input")
                        .build());

        System.out.println("requested list queue:\n");
        System.out.println("found:\n");
        for (String queueUrl : listQueuesResponse.queueUrls()) {
            System.out.println(queueUrl + "\n");
        }


        while (!shouldTerminate) {
            for (String queueUrl : listQueuesResponse.queueUrls()) {

                ReceiveMessageResponse receiveMessageResponse = sqs
                        .receiveMessage(ReceiveMessageRequest
                                .builder()
                                .queueUrl(queueUrl)
                                .messageAttributeNames("url", "analysis-type", "output-bucket")
                                .build());

                if (!receiveMessageResponse.hasMessages())
                    continue;

                Message message = receiveMessageResponse.messages().get(0);
                System.out.println("recieved message\n");
                System.out.println("body: " + message.body());
                if (message.body().equals("terminate")) {
                    shouldTerminate = true;
                    MessageOperations.deleteMessage(sqs,queueUrl, message);
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
                    String outputBucket = message.messageAttributes().get("output-bucket").stringValue();
                    System.out.printf("uploading file %s to bucket: %s\n", outputFile.getAbsolutePath(),outputBucket);
                    PutObjectResponse putObjectResponse = s3.putObject(PutObjectRequest.builder()
                                    .bucket(outputBucket)
                                    .key(message.messageId())
                                    .build(),
                            RequestBody.fromFile(outputFile));
                    System.out.println("deleting file on " + outputFile.getAbsolutePath());
                    try {
                        outputFile.delete();
                        System.out.println("file deleted");
                    }catch (Exception e){
                        System.out.println("failed to delete file");
                    }

                    System.out.println("\nsending done message\n");
                    Map<String, MessageAttributeValue> messageAttributeValueMap = new HashMap<String, MessageAttributeValue>() {{
                        put("original-url", message.messageAttributes().get("url"));
                        put("object-url", MessageAttributeValue.builder()
                                .dataType("String")
                                .stringValue(s3.utilities().getUrl(GetUrlRequest
                                        .builder()
                                        .bucket(outputBucket)
                                        .key(message.messageId())
                                        .build()).toString())
                                .build());
                        put("analysis-type", message.messageAttributes().get("analysis-type"));
                    }};

                    MessageOperations.deleteMessage(sqs, queueUrl, message);
                    MessageOperations.sendMessage(sqs, outputQueueUrl, "Success", messageAttributeValueMap);
                    System.out.println("finished successfully");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    MessageOperations.deleteMessage(sqs, queueUrl, message);
                    MessageOperations.sendMessage(sqs, outputQueueUrl, "Error: " + e.getMessage());
                }
            }
        }
        System.out.println("\nclosing resources\n");

        s3.close();
        sqs.close();
    }
}