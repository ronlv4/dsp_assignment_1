package dsp.Models;

public class SqsMessage {

    private String url;

    private SqsMessage(String url) {
        this.url = url;
    }


    public SqsMessage create(String url){
        return new SqsMessage(url);
    }

}
