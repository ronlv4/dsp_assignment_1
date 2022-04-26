package com.example.aws.ec2;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

public class Ec2Operations {

    public static String createEC2Instance(Ec2Client ec2, String name, String amiId) {
        return createEC2Instance(ec2, name, amiId, "");
    }

    public static String createEC2Instance(Ec2Client ec2, String name, String amiId, String encodedUserData) {

        IamInstanceProfileSpecification iamInstanceProfile = IamInstanceProfileSpecification.builder()
                .name("LabInstanceProfile")
                .build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .keyName("test-linux-vm")
                .userData(encodedUserData)
                .iamInstanceProfile(iamInstanceProfile)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully created EC2 Instance %s based on AMI %s\n",
                    instanceId, amiId);

            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return "";
    }

    public static void startInstance(Ec2Client ec2, String instanceId) {

        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.printf("Successfully started instance %s", instanceId);
    }

    public static void stopInstance(Ec2Client ec2, String instanceId) {

        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.stopInstances(request);
        System.out.printf("Successfully stopped instance %s", instanceId);
    }

    public static void rebootEC2Instance(Ec2Client ec2, String instanceId) {

        try {
            RebootInstancesRequest request = RebootInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.rebootInstances(request);
            System.out.printf(
                    "Successfully rebooted instance %s", instanceId);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void describeEC2Instances(Ec2Client ec2) {

        boolean done = false;
        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.println("Instance Id is " + instance.instanceId());
                        System.out.println("Image id is " + instance.imageId());
                        System.out.println("Instance type is " + instance.instanceType());
                        System.out.println("Instance state name is " + instance.state().name());
                        System.out.println("monitoring information is " + instance.monitoring().state());

                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static String getManagerInstance(Ec2Client ec2) {
        try {

            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .maxResults(6)
                    .filters(
                            Filter.builder().name("tag:Name").values("Manager").build())
                    .build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            if (!response.hasReservations())
                return createEC2Instance(ec2, "Manager", "ami-0f9fc25dd2506cf6d");
            return response.reservations().get(0).instances().get(0).instanceId();

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return "";
        }
    }
}