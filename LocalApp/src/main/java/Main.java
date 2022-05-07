import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.utils.IoUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Main {

    private static final String MANAGER_TAG = "Manager";
    private static final String AMI_ID = "ami-0f9fc25dd2506cf6d";
    private static final String BUCKET = "shir11226543666123";

    public static void main(String[] args){
        String key = "Input-File-" + UUID.randomUUID().toString();
        S3Connector s3Connector = new S3Connector(Region.US_EAST_1);
        SQSConnector sqsConnector = new SQSConnector(Region.US_EAST_1);
        EC2Connector ec2Connector = new EC2Connector(Region.US_EAST_1);
        if(args.length < 3)
            throw new IllegalArgumentException("Not enough arguments");
        String inputFileName = args[0];
        String outputFileName = args[1];
        String n = args[2];
        String terminate = args.length > 3 ? "1" : "0";
        String userData = "";
        try {
            userData = IoUtils.toUtf8String(Main.class.getResourceAsStream("/UserData.txt"));
        } catch (IOException ignored) {
        }

         ec2Connector.createEC2InstancesIfNotExists(MANAGER_TAG, AMI_ID, userData, 1);

        String queueName = "Manager-Answer-" + UUID.randomUUID().toString();
        sqsConnector.createQueue(queueName);
        Map<String, MessageAttributeValue> atts = Map.of("responseQueue", MessageAttributeValue.builder().stringValue(queueName).dataType("String").build(),
                                                         "bucket", MessageAttributeValue.builder().stringValue(BUCKET).dataType("String").build(),
                                                         "key", MessageAttributeValue.builder().stringValue(key).dataType("String").build(),
                                                         "n", MessageAttributeValue.builder().stringValue(n).dataType("String").build(),
                                                         "terminate", MessageAttributeValue.builder().stringValue(terminate).dataType("String").build());

        s3Connector.writeFileToS3(BUCKET, key, new File(inputFileName));
        sqsConnector.sendMessage("ManagerQueue", "file is in the queue", atts);

        Message m = null;
        while(Objects.isNull(m)) {
            m = sqsConnector.getMessage(queueName);
        }
        String b = m.messageAttributes().get("bucket").stringValue();
        String k = m.messageAttributes().get("key").stringValue();
        String output = s3Connector.readStringFromS3(b, k);
        sqsConnector.deleteMessage(queueName, m);
        sqsConnector.deleteQueue(queueName);
        s3Connector.deleteS3Object(BUCKET, key);
        s3Connector.deleteS3Object(b, k);
        try {
            createHtmlOutputFile(output, outputFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createHtmlOutputFile(String s, String filename) throws IOException {
        File f = new File(filename);
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write("<html><body>");
        bw.write(String.join("<br>", s.split("\n")));
        bw.write("</body></html>");
        bw.close();
    }

}
