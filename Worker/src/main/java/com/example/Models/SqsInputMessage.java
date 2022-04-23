package com.example.Models;

//import com.google.gson.Gson;

import java.io.Serializable;

public class SqsInputMessage implements Serializable {

//    public static Gson gson = new Gson();
    private String inputBucketUrl;
    private String key;

    public SqsInputMessage(String inputBucketUrl, String key) {
        this.inputBucketUrl = inputBucketUrl;
        this.key = key;
    }

    public String serializeMessage(){
//        return gson.toJson(this);
        return "";

    }

}
