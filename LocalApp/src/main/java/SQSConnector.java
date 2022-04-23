import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.stream.Collectors;

public class SQSConnector {

    private final SqsClient sqs;

    public SQSConnector(Region region){
        this.sqs = SqsClient.builder().region(region).build();
    }

    public String createQueue(String name){
        CreateQueueResponse res = sqs.createQueue(CreateQueueRequest.builder().queueName(name).build());
        return res.queueUrl();
    }

    public void deleteQueue(String name){
        String url = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build());
    }

    public void sendMessage(String name, String body){
        String url = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(url).messageBody(body).build());
    }

    public List<Message> getMessages(String name, int numOfMessages){
        String url = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
        List<Message> ans = List.of();
        ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(url)
                .maxNumberOfMessages(numOfMessages)
                .waitTimeSeconds(20)
                .build());
        if(response.hasMessages()){
            ans = response.messages();
            deleteMessages(name, ans);
        }
        return ans;
    }

    public Message getMessage(String name){
        List<Message> messages = getMessages(name, 1);
        return messages.size() > 0 ? messages.get(0) : null;
    }

    public void deleteMessages(String name, List<Message> messages){
        String url = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
        sqs.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                .queueUrl(url)
                .entries(messages.stream().map(m -> DeleteMessageBatchRequestEntry.builder().
                        id(m.messageId()).
                        receiptHandle(m.receiptHandle()).
                        build()).collect(Collectors.toList()))
                .build());
    }
}
