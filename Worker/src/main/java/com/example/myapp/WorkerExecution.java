package com.example.myapp;

import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.Callable;

public class WorkerExecution implements Callable<File> {

    Message message;
    String outputBucket;
    String outputQueueUrl;
    String fileUrl;
    String analysisType;
    S3Client s3;
    SqsClient sqs;

    static final Logger log = LogManager.getLogger();

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
        log.info("downloading from {}", url.toString());
        ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath);
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel()
                .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        log.info("file is ready at {}", outputFilePath);
        return outputFilePath;
    }

    private static File processMessage(String analysisType, String fileUrl) throws IOException {
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
                "-maxLength", "50",
                "-outputFormat", outputFormat,
                "-writeOutputFiles",
                "-outputFilesExtension", outputFileExtension,
                filePath};

        log.info("starting to parse file at {}",filePath);
        log.info("analysis type used: {}", analysisType);
        LexicalizedParser.main(parserArgs);
        String outputFilePath = outputFileDirectory + "/" + filePath + "." + outputFileExtension;
        log.info("file parsing is ready at {}", outputFilePath);

        deleteFile(new File(filePath));
        return new File(outputFilePath);
    }

    private static boolean deleteFile(File file) {
        log.debug("deleting file at {}", file.getAbsolutePath());
        return file.delete();
    }
    @Override
    public File call() throws IOException {
        return processMessage(analysisType, fileUrl);
    }
}
