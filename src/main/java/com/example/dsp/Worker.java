package com.example.dsp;

import com.example.dsp.Utils.Network;
import com.example.myapp.s3.S3BucketOps;
import com.example.myapp.s3.S3ObjectOperations;
import com.example.myapp.sqs.MessageOperations;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.parser.server.LexicalizedParserClient;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public class Worker {

    public enum AnalysisType {
        POS,
        CONSTITUENCY,
        DEPENDENCY
    }

    private String downloadTextFile(URL url){
        StringBuilder stringBuilder = new StringBuilder();
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setConnectTimeout(1000);
            urlConnection.setReadTimeout(10000);
            BufferedReader breader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
            String line;
            while((line = breader.readLine()) != null) {
                stringBuilder.append(line);
            }

            System.out.println(stringBuilder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stringBuilder.toString();
    }

    private static File parseMessage(Message message){
        String analyisType = String.valueOf(message.messageAttributes().get("analysis-type"));
        String fileUrl = String.valueOf(message.messageAttributes().get("url"));
//        String text =
        return new File("input.txt");
    }

    private static void analizeText(File textFile){
        String[] myArgs = {"-retainTMPSubcategories", "-outputFormat", "wordsAndTags,penn,typedDependencies", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz", "input.txt"};
        LexicalizedParser.main(myArgs);
        Properties props = new Properties();

    }

    public static void main(String[] args) {
        Region region = Region.US_WEST_2;

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        S3Client s3 = S3Client.builder().region(region).build();

        ListQueuesResponse listQueuesResponse = sqsClient.listQueues(ListQueuesRequest.builder().queueNamePrefix("input").build());
        for (String queueUrl: listQueuesResponse.queueUrls()){
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build());
            if (receiveMessageResponse.hasMessages()){
                Message message = receiveMessageResponse.messages().get(0);
                receiveMessageResponse.messages().remove(0);
                File outputFile = parseMessage(message);
                String outputBucket = String.valueOf(message.messageAttributes().get("bucket"));
                s3.putObject(PutObjectRequest.builder().
                        bucket(outputBucket)
                        .key(message.messageId())
                        .build(),
                        RequestBody.fromFile(outputFile));
                break;
            }
        }

    }
}
