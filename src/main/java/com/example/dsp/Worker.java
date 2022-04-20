package com.example.dsp;

import com.example.dsp.Utils.Network;
import com.example.myapp.s3.S3BucketOps;
import com.example.myapp.s3.S3ObjectOperations;
import com.example.myapp.sqs.MessageOperations;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.parser.server.LexicalizedParserClient;
import edu.stanford.nlp.trees.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public class Worker {

    public enum AnalysisType {
        POS,
        CONSTITUENCY,
        DEPENDENCY
    }

    public static File downloadTextFile(URL url) throws IOException {
        InputStream in = url.openStream();
        File outputFile = new File("output-file.txt");
        Files.copy(in, Paths.get(outputFile.toURI()), StandardCopyOption.REPLACE_EXISTING);
        return outputFile;
    }

    private static File parseMessage(Message message){
        String analyisType = String.valueOf(message.messageAttributes().get("analysis-type"));
        String fileUrl = String.valueOf(message.messageAttributes().get("url"));
//        {
//        File textFile = null;
//        try {
//            textFile = downloadTextFile(new URL(fileUrl));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        }
        return analizeText(fileUrl);
    }

    public static File analizeText(String url){
        {
        LexicalizedParser lp = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
        lp.setOptionFlags("-maxLength", "80", "-retainTmpSubcategories", "-outputFilesDirectory", ".");
        String[] sent = { "This", "is", "an", "easy", "sentence", "." };
        List<CoreLabel> rawWords = Sentence.toCoreLabelList(sent);
        Tree parse = lp.apply(rawWords);
        parse.pennPrint();
        System.out.println();
        TreebankLanguagePack tlp = new PennTreebankLanguagePack();
        GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
        GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
        List<TypedDependency> tdl = gs.typedDependenciesCCprocessed();
        System.out.println(tdl);
        System.out.println();
        TreePrint tp = new TreePrint("penn,typedDependenciesCollapsed");
        tp.printTree(parse);
        }
        {
        String[] myArgs = {"-model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz", "-saveToTextFile", "./output.txt", "-retainTMPSubcategories", "-outputFormat", "wordsAndTags,penn,typedDependencies", url};
        LexicalizedParser.main(myArgs);
        }
        System.exit(0);
        return new File("sd");
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
