import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static S3Connector s3Connector = new S3Connector(Region.US_EAST_1);
    public static SQSConnector sqsConnector = new SQSConnector(Region.US_EAST_1);
    public static EC2Connector ec2Connector = new EC2Connector(Region.US_EAST_1);


    public static void main(String[] args){

        ExecutorService executor = Executors.newFixedThreadPool(4);
        boolean running = true;
        while(running) {
            Message m = sqsConnector.getMessage("ManagerQueue");
            if(Objects.nonNull(m)) {
                if(m.messageAttributes().get("terminate").stringValue().equals("1"))
                    running = false;
                executor.submit(() -> handleRequest(m));
//                handleRequest(m);
            }
        }
    }

    private static void handleRequest(Message m){
        System.out.println(m.messageAttributes());
        double n = Double.parseDouble(m.messageAttributes().get("n").stringValue());
        String bucket = m.messageAttributes().get("bucket").stringValue();
        String key = m.messageAttributes().get("key").stringValue();
        String responseQueue = m.messageAttributes().get("responseQueue").stringValue();
        String inp = s3Connector.readStringFromS3(bucket, key);
        String[] lines = inp.split("\\r?\\n");
        int neededWorkers = Math.min((int)Math.ceil(lines.length/n), 19);
        // ec2Connector.createEC2InstancesIfNotExists("Worker", "ami-0f9fc25dd2506cf6d", "asd", neededWorkers);

        String queueName = UUID.randomUUID().toString();

        for(String line : lines){
            System.out.println(line);
            String analysis = line.split("\t")[0];
            String fileUrl = line.split("\t")[1];

            sqsConnector.sendMessage("WorkerQueue", "a new job", Map.of("responseQueue", MessageAttributeValue.builder().stringValue(queueName).dataType("String").build(),
                    "fileUrl", MessageAttributeValue.builder().stringValue(fileUrl).dataType("String").build(),
                    "analysis", MessageAttributeValue.builder().stringValue(analysis).dataType("String").build()));
        }

        int receivedMessages = 0;
        StringBuilder ans = new StringBuilder();
        while(receivedMessages < lines.length){
            Message workerMessage = sqsConnector.getMessage("WorkerQueue");
            if(Objects.nonNull(workerMessage)){
                receivedMessages += 1;
//                String outputUrl = workerMessage.messageAttributes().get("outputUrl").stringValue();
//                String inputUrl = workerMessage.messageAttributes().get("inputUrl").stringValue();
                String fileUrl = workerMessage.messageAttributes().get("fileUrl").stringValue();
                String analysis = workerMessage.messageAttributes().get("analysis").stringValue();
                ans.append(String.format("%s: %s %s\n", analysis, fileUrl, fileUrl));
            }
        }

        String responseKey = UUID.randomUUID().toString();
        s3Connector.writeStringToS3(bucket, responseKey, ans.toString());
        sqsConnector.sendMessage(responseQueue, "a", Map.of("bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                "key", MessageAttributeValue.builder().stringValue(responseKey).dataType("String").build()));
    }

}
