package com.MQ.core;


import com.MQ.Controllers.TopicController;
import com.MQ.Exception.PartitionIsEmptyException;
import com.MQ.Models.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Partition {
    private static final Logger logger = LogManager.getLogger(Partition.class);

    private final Topic topicName;
    private String partitionId;

    private final StorageStrategy<Message> storage;


    public Partition(String partitionId, Topic topicName) {
        this.partitionId = partitionId;
        this.storage = new ArrayStorage();
        this.topicName=topicName;
    }

    public void addMessage(Message message)
    {
        logger.info("writing message "+message.getMessage() +" in parition "+partitionId);
        storage.storeElement(message);
    }

    public Message getForOffset(int offset) throws PartitionIsEmptyException {

        return storage.getElement(offset);
    }
}
