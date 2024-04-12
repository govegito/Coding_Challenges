package com.MQ.core;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class Topic {

    private final String topicName;
    private final int partitionCount;
    private final TopicPartitionBalancingStrategy strategy;

    public Topic(String topicName, int partitionCount) {
        this.topicName = topicName;
        this.partitionCount=partitionCount;
        try {
            this.strategy = new TopicPartitionBalancingStrategy(partitionCount);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTopicName() {
        return topicName;
    }

    public String getAssignedPartition(String key)
    {
        int part;
        if(key==null || key.equalsIgnoreCase(""))
            part=strategy.getNext();
        else
            part=strategy.getForKey(key);

        return this.topicName+"-P"+part;

    }

    public List<String> getAllPartition(){

        List<String> list = new ArrayList<>();

       for(int i=0;i<partitionCount;i++)
        list.add(topicName+"-P"+i);

       return list;

    }
}
