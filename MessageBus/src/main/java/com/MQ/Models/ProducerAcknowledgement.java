package com.MQ.Models;

public class ProducerAcknowledgement {

    private String messageId;

    private String acknowledgementMessage;

    public ProducerAcknowledgement() {
    }

    public ProducerAcknowledgement(String messageId, String acknowledgementMessage) {
        this.messageId = messageId;
        this.acknowledgementMessage = acknowledgementMessage;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getAcknowledgementMessage() {
        return acknowledgementMessage;
    }

    public void setAcknowledgementMessage(String acknowledgementMessage) {
        this.acknowledgementMessage = acknowledgementMessage;
    }
}
