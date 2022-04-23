import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Objects;

public class Manager {

    public static void main(String[] args){
        String bucket = "shir11226543666123";
        String key = "shir";
        S3Connector s3Connector = new S3Connector(Region.US_EAST_1);
        SQSConnector sqsConnector = new SQSConnector(Region.US_EAST_1);

        Message m = null;
        while(Objects.isNull(m)) {
            m = sqsConnector.getMessage("shir");
            System.out.println(System.currentTimeMillis());
        }

        String s = s3Connector.readStringFromS3(bucket, key);
        System.out.println(s);
        sqsConnector.sendMessage("shir1", s);



    }

}
