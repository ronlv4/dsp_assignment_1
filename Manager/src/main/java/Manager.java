import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Manager {
    private static final String WORKER_TAG = "Worker";
    private static final String AMI_ID = "ami-0f9fc25dd2506cf6d";
    static S3Connector s3Connector = new S3Connector(Region.US_EAST_1);
    static SQSConnector sqsConnector = new SQSConnector(Region.US_EAST_1);
    static EC2Connector ec2Connector = new EC2Connector(Region.US_EAST_1);
    static ExecutorService executor = Executors.newFixedThreadPool(8);

    static String userData;

    static {
        try {
            userData = IoUtils.toUtf8String(Manager.class.getResourceAsStream("/workerUserData.txt"));
        } catch (IOException e) {
            userData = "";
        }
    }


    public static void main(String[] args){

        boolean running = true;
        while(running) {
            Message m = sqsConnector.getMessage("ManagerQueue");
            if(Objects.nonNull(m)) {
                if(m.messageAttributes().get("terminate").stringValue().equals("1"))
                    running = false;
                executor.submit(() -> handleRequest(m));
            }
        }
        terminate();
    }

    private static void handleRequest(Message m){
        System.out.println(m.messageAttributes());
        double n = Double.parseDouble(m.messageAttributes().get("n").stringValue());
        String bucket = m.messageAttributes().get("bucket").stringValue();
        String key = m.messageAttributes().get("key").stringValue();
        String responseQueue = m.messageAttributes().get("responseQueue").stringValue();
        String inp = s3Connector.readStringFromS3(bucket, key);
        String[] lines = inp.split("\\r?\\n");
        int neededWorkers = Math.min((int)Math.ceil(lines.length/n), 16);
        ec2Connector.createEC2InstancesIfNotExists(WORKER_TAG, AMI_ID, userData, neededWorkers);


        String queueName = "Worker-Answer-" + UUID.randomUUID().toString();
        sqsConnector.createQueue(queueName);
        Arrays.stream(lines).parallel().forEach(line -> {
            System.out.println(line);
            String analysis = line.split("\t")[0];
            String fileUrl = line.split("\t")[1];

            sqsConnector.sendMessage("WorkerQueue", "a new job", Map.of("responseQueue", MessageAttributeValue.builder().stringValue(queueName).dataType("String").build(),
                    "fileUrl", MessageAttributeValue.builder().stringValue(fileUrl).dataType("String").build(),
                    "analysis", MessageAttributeValue.builder().stringValue(analysis).dataType("String").build(),
                    "bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build()));
        });

        int receivedMessages = 0;
        StringBuilder ans = new StringBuilder();
        while(receivedMessages < lines.length){
            Message workerMessage = sqsConnector.getMessage(queueName);
            if(Objects.nonNull(workerMessage)){
                receivedMessages += 1;
                String outputUrl = workerMessage.messageAttributes().get("outputUrl").stringValue();
                String inputUrl = workerMessage.messageAttributes().get("inputUrl").stringValue();
                String analysis = workerMessage.messageAttributes().get("analysis").stringValue();
                ans.append(String.format("%s: %s %s\n", analysis, inputUrl, outputUrl));
                sqsConnector.deleteMessage(queueName, workerMessage);
            }
        }

        String responseKey = "Output-File-" + UUID.randomUUID().toString();
        s3Connector.writeStringToS3(bucket, responseKey, ans.toString());
        sqsConnector.sendMessage(responseQueue, "a", Map.of("bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                "key", MessageAttributeValue.builder().stringValue(responseKey).dataType("String").build()));

        sqsConnector.deleteMessage("ManagerQueue", m);
        sqsConnector.deleteQueue(queueName);
    }

    private static void terminate(){
        try {
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.MINUTES);
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        }
        finally {
            List<Instance> workersToTerminate = ec2Connector.getInstancesWithTag(WORKER_TAG);
            int terminated = ec2Connector.terminateInstances(workersToTerminate.stream().map(Instance::instanceId).collect(Collectors.toList()));
            if(terminated < workersToTerminate.size())
                System.out.println("Not all workers were terminated!");
            ec2Connector.terminateInstances(List.of(EC2MetadataUtils.getInstanceId()));
        }
    }
}
