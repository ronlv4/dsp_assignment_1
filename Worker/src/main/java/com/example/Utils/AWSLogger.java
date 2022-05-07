package com.example.Utils;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DeleteLogStreamRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import software.amazon.awssdk.regions.Region;

import java.util.Calendar;

public class AWSLogger {

    private final AWSLogs awsLogs;

    public AWSLogger(Region region){
        this.awsLogs = AWSLogsClientBuilder.standard().withRegion(region.toString()).build();
    }

    public void createLogStream(String name){
        awsLogs.createLogStream(new CreateLogStreamRequest()
                .withLogGroupName("Worker")
                .withLogStreamName(name));
    }

    public void deleteLogStream(String name){
        awsLogs.deleteLogStream(new DeleteLogStreamRequest()
                .withLogGroupName("Worker")
                .withLogStreamName(name));
    }

    public void writeLog(String name, String message){
        awsLogs.putLogEvents(new PutLogEventsRequest()
                .withLogGroupName("Worker")
                .withLogStreamName(name)
                .withLogEvents(new InputLogEvent()
                        .withMessage(message)
                        .withTimestamp(Calendar.getInstance().getTimeInMillis())));
    }

}
