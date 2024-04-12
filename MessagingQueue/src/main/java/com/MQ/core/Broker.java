package com.MQ.core;

import com.MQ.Controllers.TopicController;
import com.MQ.Exception.PartitionIsEmptyException;
import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Models.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class Broker {
    private static final Logger logger = LogManager.getLogger(Broker.class);

    private String brokerID;

    private Map<String,Partition> partitionMap;

    public Broker(String brokerID) {

        this.brokerID = brokerID;
        this.partitionMap=new HashMap<>();
    }

    public String getBrokerID() {
        return brokerID;
    }

    public void setBrokerID(String brokerID) {
        this.brokerID = brokerID;
    }

    public void addPartition(String partitionId, Topic topic) {
        this.partitionMap.put(partitionId,new Partition(partitionId,topic));
    }

    public void writeToPartition(String partitionId, Message message) throws PartitionNotFoundException {

        if(partitionMap.containsKey(partitionId))
        {
            partitionMap.get(partitionId).addMessage(message);
        }
        else
            throw new PartitionNotFoundException("No partition found in broker "+ brokerID +" for partition "+partitionId);
    }

    public Message getMessageFromPartition(String partitionId, int offset) throws PartitionNotFoundException, PartitionIsEmptyException {

        if(!partitionMap.containsKey(partitionId))
            throw new PartitionNotFoundException("No partition found in broker "+ brokerID +" for partition "+partitionId);

        return partitionMap.get(partitionId).getForOffset(offset);

    }
}
