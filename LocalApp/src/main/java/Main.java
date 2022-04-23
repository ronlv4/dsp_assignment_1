import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Objects;

public class Main {

    public static void main(String[] args){
        String bucket = "shir11226543666123";
        String key = "shir";
        S3Connector s3Connector = new S3Connector(Region.US_EAST_1);
        SQSConnector sqsConnector = new SQSConnector(Region.US_EAST_1);
        if(args.length < 3)
            throw new IllegalArgumentException("Not enough arguments");
        String inputFileName = args[0];
        String outputFileName = args[1];
        Integer n = Integer.parseInt(args[2]);
        boolean terminate = args.length > 3;

        s3Connector.writeFileToS3(bucket, key, inputFileName);
        sqsConnector.sendMessage("shir", "file is in the queue");

        Message m = null;
        while(Objects.isNull(m)) {
            m = sqsConnector.getMessage("shir1");
            System.out.println(System.currentTimeMillis());
        }
        System.out.println(m.body());
    }

}
