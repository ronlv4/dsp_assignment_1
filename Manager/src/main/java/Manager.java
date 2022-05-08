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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class Manager {
    private static final String WORKER_TAG = "Worker";
    private static final String WORKER_QUEUE = "WorkerQueue";
    private static final String WORKER_ANSWER_QUEUE = "WorkerAnswerQueue";
    private static final String MANAGER_QUEUE = "ManagerQueue";
    private static final String AMI_ID = "ami-0f9fc25dd2506cf6d";
    static S3Connector s3Connector = new S3Connector(Region.US_EAST_1);
    static SQSConnector sqsConnector = new SQSConnector(Region.US_EAST_1);
    static EC2Connector ec2Connector = new EC2Connector(Region.US_EAST_1);
    static ExecutorService executor = Executors.newFixedThreadPool(8);
    static Map<String, ArrayList<Message>> messagesFromWorkers = new HashMap<>();
    static ReadWriteLock lock = new ReentrantReadWriteLock();

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
        new Thread(Manager::waitForAnswers).start();
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
        String responseQueue = m.messageAttributes().get("responseQueue").stringValue();

        String inp = s3Connector.readStringFromS3(bucket, key);
        String[] lines = inp.split("\\r?\\n");
        int neededWorkers = Math.min((int)Math.ceil(lines.length/n), 16);
        ec2Connector.createEC2InstancesIfNotExists(WORKER_TAG, AMI_ID, userData, neededWorkers);

        log.info(String.format("Answers for file %s will be written to id %s", key, responseQueue));
        messagesFromWorkers.put(responseQueue, new ArrayList<>(Arrays.asList(new Message[lines.length])));
        for(int i = 0;i < lines.length;i++){
            String analysis = lines[i].split("\t")[0];
            String fileUrl = lines[i].split("\t")[1];

            sqsConnector.sendMessage(WORKER_QUEUE, "a new job", Map.of(
                    "responseQueue", MessageAttributeValue.builder().stringValue(WORKER_ANSWER_QUEUE).dataType("String").build(),
                    "answerId", MessageAttributeValue.builder().stringValue(responseQueue).dataType("String").build(),
                    "fileUrl", MessageAttributeValue.builder().stringValue(fileUrl).dataType("String").build(),
                    "analysis", MessageAttributeValue.builder().stringValue(analysis).dataType("String").build(),
                    "bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                    "order", MessageAttributeValue.builder().stringValue(Integer.toString(i)).dataType("String").build()));
        }
    }

    private static void returnAnswer(ArrayList<Message> messages, String responseQueue){
        String bucket = "";
        StringBuilder ans = new StringBuilder();
        for(Message m : messages){
            String outputUrl = m.messageAttributes().get("outputUrl").stringValue();
            String inputUrl = m.messageAttributes().get("inputUrl").stringValue();
            String analysis = m.messageAttributes().get("analysis").stringValue();
            bucket = m.messageAttributes().get("bucket").stringValue();
            ans.append(String.format("%s: %s %s\n", analysis, inputUrl, outputUrl));
        }

        String responseKey = "Output-File-" + UUID.randomUUID().toString();
        s3Connector.writeStringToS3(bucket, responseKey, ans.toString());
        log.info(String.format("Writing file %s to s3", responseKey));
        sqsConnector.sendMessage(responseQueue, "a", Map.of("bucket", MessageAttributeValue.builder().stringValue(bucket).dataType("String").build(),
                "key", MessageAttributeValue.builder().stringValue(responseKey).dataType("String").build()));

    }

    private static void terminate(){
        log.info("Started termination process");
        while(!messagesFromWorkers.isEmpty());
        log.info("All message ids were handled");
        try {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
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

    private static void waitForAnswers(){
        while(true){
            Message m = sqsConnector.getMessage(WORKER_ANSWER_QUEUE);
            if(Objects.nonNull(m)) {
                executor.submit(() -> sqsConnector.deleteMessage(WORKER_ANSWER_QUEUE, m));
                String id = m.messageAttributes().get("answerId").stringValue();
                int order = Integer.parseInt(m.messageAttributes().get("order").stringValue());
                log.info(String.format("Got response with id %s and order %s", id, order));
                if(messagesFromWorkers.containsKey(id)){
                    messagesFromWorkers.get(id).set(order, m);
                    if(messagesFromWorkers.get(id).stream().noneMatch(Objects::isNull)){
                        log.info(String.format("Got all responses for id %s", id));
                        executor.submit(() -> returnAnswer(messagesFromWorkers.get(id), id));
                        messagesFromWorkers.remove(id);
                        log.info(String.format("%d unfinished tasks left", messagesFromWorkers.size()));
                    }
                }
            }
        }
    }
}
