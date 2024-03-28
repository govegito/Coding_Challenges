package com.MQ.Models;

import java.sql.Timestamp;

public class Message {

    private final String message;

    private final String key;

    private final String topicName;

    private final Timestamp timestamp;

    public Message(String message, String key, String topicName, Timestamp timestamp) {
        this.message = message;
        this.key = key;
        this.topicName = topicName;
        this.timestamp = timestamp;
    }

    public Message(MessagePayload messagePayload, String topicName) {
        this.message = messagePayload.getMessage();
        this.key = messagePayload.getKey();
        this.topicName = topicName;
        this.timestamp = messagePayload.getTimestamp();
    }


    public String getMessage() {
        return message;
    }

    public String getKey() {
        return key;
    }

    public String getTopicName() {
        return topicName;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }
}
