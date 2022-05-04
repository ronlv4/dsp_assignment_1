package com.example.myapp;

import com.example.aws.sqs.MessageOperations;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.concurrent.Callable;

public class WorkerExecution implements Callable<File> {

    Message message;
    String outputBucket;
    String outputQueueUrl;
    String fileUrl;
    String analysisType;
    S3Client s3;
    SqsClient sqs;

    public WorkerExecution(Message queueMessage, S3Client s3Client, SqsClient sqsClient) {
        message = queueMessage;
        outputBucket = message.messageAttributes().get("bucket").stringValue();
        outputQueueUrl = message.messageAttributes().get("responseQueue").stringValue();
        fileUrl = message.messageAttributes().get("fileUrl").stringValue();
        analysisType = message.messageAttributes().get("analysis").stringValue();
        s3 = s3Client;
        sqs = sqsClient;
    }

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

    @Override
    public File call() throws IOException {
        return processMessage(message, analysisType, fileUrl);
    }
}
