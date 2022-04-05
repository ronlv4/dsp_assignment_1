import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

public class Manager {

    public static String[] downloadInputFromS3(String url){
        return new String[2];
    }

    public static void main(String[] args) {
        String inputURL = args[0];
        String[] textFilesUrls = downloadInputFromS3(inputURL);

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName()
                .build();

        sqsClient.createQueue(createQueueRequest);




    }
}
