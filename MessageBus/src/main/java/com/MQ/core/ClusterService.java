package com.MQ.core;

import com.MQ.Config.ProducerConfig;
import com.MQ.Controllers.ProducerController;
import com.MQ.Exception.PartitionIsEmptyException;
import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.Models.Message;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClusterService {

    private static final Logger logger = LogManager.getLogger(ClusterService.class);

    private final List<Broker> brokerList;

    private Map<String,Topic> topicList;

    private Map<String,Broker> partionToBroker;

    private final BrokerPartitionBalancingStrategy balancingStrategy;

    @Autowired
    private ProducerConfig producerConfig;


    public ClusterService() {
        this.brokerList = new ArrayList<>();
        this.partionToBroker = new HashMap<>();
        this.topicList= new HashMap<>();
        this.balancingStrategy = new BrokerPartitionBalancingStrategy();
    }

    @PostConstruct
    public void init(){

        for(int i=0;i<producerConfig.getBrokerNumber();i++)
        {
            this.brokerList.add(new Broker("broker"+ i));
        }
    }

    public void addTopic(Topic topic)
    {
        this.topicList.put(topic.getTopicName(),topic);
        this.balancingStrategy.balanceForTopic(topic,brokerList,partionToBroker,producerConfig.getPartitionNumber());
    }

    public void writeMessageToTopic(Message message) throws PartitionNotFoundException, TopicNotFoundException {
        if(this.topicList.containsKey(message.getTopicName()))
        {
            String partitionId=this.topicList.get(message.getTopicName()).getAssignedPartition(message.getKey());
            logger.info(" Topic found and the next partition is "+ partitionId);
            if(partionToBroker.containsKey(partitionId)){

                partionToBroker.get(partitionId).writeToPartition(partitionId,message);
            }
            else
                throw new PartitionNotFoundException("No partition found for partition id "+partitionId);
        }
        else
            throw new TopicNotFoundException("No topic found for the name "+message.getTopicName());
    }

    public boolean ifTopicPresent(String topicName)
    {
        return topicList.containsKey(topicName);
    }

    public Message getMessageFromPartition(String partitionId, int offset) throws PartitionNotFoundException, PartitionIsEmptyException {
        logger.info(" get message from partition id "+partitionId +" for the offset "+offset);

        if(!partionToBroker.containsKey(partitionId))
            throw new PartitionNotFoundException("No partition found for partition id "+partitionId);

        return partionToBroker.get(partitionId).getMessageFromPartition(partitionId,offset);

    }

    public List<String> getPartitionForTopic(String topic) throws TopicNotFoundException {

        if(topicList.containsKey(topic))
        {
            return topicList.get(topic).getAllPartition();

        }
        else
            throw new TopicNotFoundException("No topic found for "+topic);
    }
}
