import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class EC2Connector {

    private final Ec2Client ec2;


    public EC2Connector(Region region){
        this.ec2 = Ec2Client.builder().region(region).build();
    }

    public void createEC2InstancesIfNotExists(String tagName, String amiId, String userData, int n) {
        int instances = getInstancesWithTag(tagName);
        int instancesToCreate = Math.max(0, n - instances);
        for(int i = 0;i < instancesToCreate;i++){
            createEC2Instance(tagName, amiId, userData);
        }
    }

    public void createEC2Instance(String tagName, String amiId, String userData) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .keyName("dist1")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::350086659594:instance-profile/LabInstanceProfile").build())
                .userData(new String(Base64.getEncoder().encode(userData.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8))
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key("Name").value(tagName).build())
                        .build())

                .build();
        ec2.runInstances(runRequest);
    }

    public int getInstancesWithTag(String tagName){
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .filters(Filter.builder().name("tag:Name").values(tagName).build())
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(describeInstancesRequest);
        return response.reservations().size();
    }
}
