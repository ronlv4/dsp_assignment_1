import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.utils.IoUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Main {

    public static void main(String[] args){
        String bucket = "shir11226543666123";
        String key = "shir";
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

        // ec2Connector.createEC2InstancesIfNotExists("Manager", "ami-0f9fc25dd2506cf6d", userData, 1);

        String queueName = UUID.randomUUID().toString();
        sqsConnector.createQueue(queueName);
        Map<String, MessageAttributeValue> atts = Map.of("responseQueue", MessageAttributeValue.builder().stringValue(queueName).dataType("String").build(),
                                                         "bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                                                         "key", MessageAttributeValue.builder().stringValue(key).dataType("String").build(),
                                                         "n", MessageAttributeValue.builder().stringValue(n).dataType("String").build(),
                                                         "terminate", MessageAttributeValue.builder().stringValue(terminate).dataType("String").build());

        s3Connector.writeFileToS3(bucket, key, new File(inputFileName));
        sqsConnector.sendMessage("ManagerQueue", "file is in the queue", atts);

        Message m = null;
        while(Objects.isNull(m)) {
            m = sqsConnector.getMessage(queueName);
        }
        String b = m.messageAttributes().get("bucket").stringValue();
        String k = m.messageAttributes().get("key").stringValue();
        System.out.println(s3Connector.readStringFromS3(b, k));
    }
}
