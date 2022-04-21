package com.dsp.instances;

import com.dsp.aws.sqs.MessageOperations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static File processMessage(Message message) {
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

    public static File analizeText(String url) {
        {
            LexicalizedParser lp = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
            lp.setOptionFlags("-maxLength", "80", "-retainTmpSubcategories", "-outputFilesDirectory", ".");
            String[] sent = {"This", "is", "an", "easy", "sentence", "."};
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
        for (String queueUrl : listQueuesResponse.queueUrls()) {
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build());
            if (receiveMessageResponse.hasMessages()) {
                Message message = receiveMessageResponse.messages().get(0);
                String outputQueueUrl = sqsClient.listQueues(ListQueuesRequest.builder().queueNamePrefix("output").maxResults(1).build()).queueUrls().get(0);
                try {
                    File outputFile = processMessage(message);
                    String outputBucket = String.valueOf(message.messageAttributes().get("bucket"));
                    PutObjectResponse putObjectResponse = s3.putObject(PutObjectRequest.builder().
                                    bucket(outputBucket)
                                    .key(message.messageId())
                                    .build(),
                            RequestBody.fromFile(outputFile));
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

                    MessageOperations.sendMessage(sqsClient, outputQueueUrl, "Success", messageAttributeValueMap);
                } catch (Exception e){
                    MessageOperations.sendMessage(sqsClient, outputQueueUrl, "Error"); // #TODO Send description of error
                }

                receiveMessageResponse.messages().remove(0);
                break;
            }
        }
    }
}
