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
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
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

    private static File processMessage(Message message, String analysisType, String fileUrl) throws IOException {
        String filePath = donwloadFile(new URL(fileUrl));
        return analyzeText(filePath, analysisType);
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
                "-outputFormat", outputFormat,
                "-writeOutputFiles",
                "-outputFilesExtension", outputFileExtension,
                filePath};

        LexicalizedParser.main(parserArgs);

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

        String inputQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest
                .builder()
                .queueName("WorkerQueue")
                .build()).queueUrl();

        while (!shouldTerminate) {
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
                shouldTerminate = true;
                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);
                continue;
            }

            String outputBucket = message.messageAttributes().get("bucket").stringValue();
            String outputQueueUrl = message.messageAttributes().get("responseQueue").stringValue();
            String fileUrl = message.messageAttributes().get("fileUrl").stringValue();
            String analysisType = message.messageAttributes().get("analysis").stringValue();

            try {
                File outputFile = processMessage(message, analysisType, fileUrl);

                s3.putObject(PutObjectRequest.builder()
                                .bucket(outputBucket)
                                .key(message.messageId())
                                .build(),
                        RequestBody.fromFile(outputFile));

                String outputUrl = s3.utilities().getUrl(GetUrlRequest
                        .builder()
                        .bucket(outputBucket)
                        .key(message.messageId())
                        .build()).toString();

                MessageOperations.sendMessage(sqs, outputQueueUrl, "Success",
                        Map.of("outputUrl", MessageAttributeValue.builder().dataType("String").stringValue(outputUrl).build(),
                                "inputUrl", message.messageAttributes().get("fileUrl"),
                                "analysis", message.messageAttributes().get("analysis")));

                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);

            } catch (Exception e) {
                MessageOperations.deleteMessage(sqs, inputQueueUrl, message);
                MessageOperations.sendMessage(sqs, outputQueueUrl, "Error: " + e.getMessage());
            }
        }
        s3.close();
        sqs.close();
        Ec2Client.builder().build().terminateInstances(TerminateInstancesRequest.builder().instanceIds(EC2MetadataUtils.getInstanceId()).build());
    }
}