import com.amazonaws.util.EC2MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.utils.IoUtils;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Manager {
    private static final String WORKER_TAG = "Worker";
    private static final String WORKER_QUEUE = "WorkerQueue";
    private static final String MANAGER_QUEUE = "ManagerQueue";
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
        log.info("Manager started running");
        while(running) {
            Message m = sqsConnector.getMessage(MANAGER_QUEUE);
            if(Objects.nonNull(m)) {
                if(m.messageAttributes().get("terminate").stringValue().equals("1"))
                    running = false;
                executor.submit(() -> handleRequest(m));
            }
        }
        terminate();
        log.info("Manager stopped running");
    }

    private static void handleRequest(Message m){
        sqsConnector.deleteMessage(MANAGER_QUEUE, m);

        double n = Double.parseDouble(m.messageAttributes().get("n").stringValue());
        String bucket = m.messageAttributes().get("bucket").stringValue();
        String key = m.messageAttributes().get("key").stringValue();
        String appResponseQueueName = m.messageAttributes().get("responseQueueName").stringValue();

        String inp = s3Connector.readStringFromS3(bucket, key);
        String[] lines = inp.split("\\r?\\n");
        int neededWorkers = Math.min((int)Math.ceil(lines.length/n), 16);
        ec2Connector.createEC2InstancesIfNotExists(WORKER_TAG, AMI_ID, userData, neededWorkers);

        String workerResponseQueueName = "Worker-Answer-" + UUID.randomUUID().toString();
        log.info(String.format("Answers for file %s will be written to queue %s", key, workerResponseQueueName));
        Set<String> neededAnswers = new HashSet<>();
        sqsConnector.createQueue(workerResponseQueueName);
        for(int i = 0;i < lines.length;i++){
            String analysis = lines[i].split("\t")[0];
            String fileUrl = lines[i].split("\t")[1];
            neededAnswers.add(Integer.toString(i));

            sqsConnector.sendMessage(WORKER_QUEUE, "a new job", Map.of("responseQueueName", MessageAttributeValue.builder().stringValue(workerResponseQueueName).dataType("String").build(),
                    "fileUrl", MessageAttributeValue.builder().stringValue(fileUrl).dataType("String").build(),
                    "analysis", MessageAttributeValue.builder().stringValue(analysis).dataType("String").build(),
                    "bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                    "order", MessageAttributeValue.builder().stringValue(Integer.toString(i)).dataType("String").build()));
        }

        Set<String> answers = new HashSet<>();
        StringBuilder ans = new StringBuilder();
        while(neededAnswers.size() != answers.size()){
            Message workerMessage = sqsConnector.getMessage(workerResponseQueueName);
            if(Objects.nonNull(workerMessage)){
                String order = workerMessage.messageAttributes().get("order").stringValue();
                if(answers.contains(order))
                    continue;
                answers.add(order);
                String outputUrl = workerMessage.messageAttributes().get("outputUrl").stringValue();
                String inputUrl = workerMessage.messageAttributes().get("inputUrl").stringValue();
                String analysis = workerMessage.messageAttributes().get("analysis").stringValue();
                ans.append(String.format("%s: %s %s\n", analysis, inputUrl, outputUrl));
                sqsConnector.deleteMessage(workerResponseQueueName, workerMessage);
            }
        }

        String responseKey = "Output-File-" + UUID.randomUUID().toString();
        s3Connector.writeStringToS3(bucket, responseKey, ans.toString());
        log.info(String.format("Writing file %s to s3", responseKey));
        sqsConnector.sendMessage(appResponseQueueName, "a", Map.of("bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                "key", MessageAttributeValue.builder().stringValue(responseKey).dataType("String").build()));

        sqsConnector.deleteQueue(workerResponseQueueName);
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
            log.info(String.format("Sending %d termination messages", workersToTerminate.size()));
            for(int i = 0;i < workersToTerminate.size(); i++){
                sqsConnector.sendMessage(WORKER_QUEUE, "terminate", Map.of());
            }
            ec2Connector.terminateInstances(List.of(EC2MetadataUtils.getInstanceId()));
        }
    }
}
