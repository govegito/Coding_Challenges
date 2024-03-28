package com.MQ.Models;

import java.sql.Timestamp;

public class MessagePayload {

    private String messageID;
    private String key;
    private String message;
    private Timestamp timestamp;

    public MessagePayload() {
    }

    public MessagePayload(String key, String id, Timestamp timestamp, String messageID) {
        this.key = key;
        this.message = id;
        this.timestamp = timestamp;
        this.messageID =messageID;
    }

    public String getMessageID() {
        return messageID;
    }

    public void setMessageID(String messageID) {
        this.messageID = messageID;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString()
    {
        return this.messageID+" "+
                this.key+" "+
                this.message+" "+
                this.timestamp;
    }
}
