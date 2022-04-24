package com.example.myapp;

import com.example.aws.ec2.Ec2Operations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Base64;

public class TestWorker {


    public static void main(String[] args) throws IOException {
        Region region = Region.US_WEST_2;
        System.exit(0);

        Ec2Client ec2 = Ec2Client.builder().region(region).build();

        byte[] userDataScriptAsBytes = Files.readAllBytes(FileSystems.getDefault().getPath("Manager/managerUserData"));
        String encodedUserDataScript = Base64.getEncoder().encodeToString(userDataScriptAsBytes);

        String instanceId = Ec2Operations.createEC2Instance(ec2, "Worker", "ami-02b92c281a4d3dc79", encodedUserDataScript);
        Ec2Operations.startInstance(ec2, instanceId);


    }


}
