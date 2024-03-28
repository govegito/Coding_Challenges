package com.MQ.Models;

public class ConsumerMessagePayload {

    private final Message message;
    private final Long messageId;

    public ConsumerMessagePayload(Message message, Long messageId) {
        this.message = message;
        this.messageId = messageId;
    }

    public Message getMessage() {
        return message;
    }

    public Long getMessageId() {
        return messageId;
    }
}
