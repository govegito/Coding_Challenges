package com.MQ.core;

import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class OffSetManager {

    private volatile Map<String, Map<String,Integer>> partitionOffsetPerConsumerGroup;
    private volatile Map<String, Map<String,Boolean>> topicPerConsumerGroup;
    private ReentrantLock lock = new ReentrantLock();

    @Autowired
    private ClusterService clusterService;
    @Autowired
    public OffSetManager() {
        this.partitionOffsetPerConsumerGroup=new HashMap<>();
        this.topicPerConsumerGroup=new HashMap<>();
    }

    public void registerConsumerGroup(String consumerGroupId){
        try{
            lock.lock();
            topicPerConsumerGroup.put(consumerGroupId,new HashMap<>());
            partitionOffsetPerConsumerGroup.put(consumerGroupId,new HashMap<>());
        }finally {
            lock.unlock();
        }
    }

    public void addTopicToConsumer(String consumerGroupId, String topic) throws ConsumerGroupNotFoundException, TopicNotFoundException {
         try{
                 lock.lock();
                 if(!(topicPerConsumerGroup.containsKey(consumerGroupId) && partitionOffsetPerConsumerGroup.containsKey(consumerGroupId))) {
                     throw new ConsumerGroupNotFoundException("No consumer registered in offset client "+consumerGroupId);
                 }
                 if(topicPerConsumerGroup.get(consumerGroupId).containsKey(topic)) {
                     return;
                 }
                 else {
                     for(String partitionId: clusterService.getPartitionForTopic(topic)) {
                         partitionOffsetPerConsumerGroup.get(consumerGroupId).put(partitionId,0);
                     }
                     topicPerConsumerGroup.get(consumerGroupId).put(topic,true);
                 }

         }finally {
             lock.unlock();
         }
     }

     public void checkOffset(String consumerGroupId, String partitionId) throws ConsumerGroupNotFoundException, PartitionNotFoundException {
             if(!partitionOffsetPerConsumerGroup.containsKey(consumerGroupId)) {
                 throw new ConsumerGroupNotFoundException("No consumer registered in offset client");
             }
             if(!partitionOffsetPerConsumerGroup.get(consumerGroupId).containsKey(partitionId))
                 throw new PartitionNotFoundException("No partition found for consumer id "+ consumerGroupId + " and partitionId "+partitionId);

         }


     public int getOffset(String consumerGroupId, String partitionId) throws PartitionNotFoundException, ConsumerGroupNotFoundException {
        try{
            lock.lock();
            checkOffset(consumerGroupId,partitionId);
            return partitionOffsetPerConsumerGroup.get(consumerGroupId).get(partitionId);
        }
        finally {
            lock.unlock();
        }
     }

    public void commitOffset(String consumerGroupId, String partitionId) throws PartitionNotFoundException, ConsumerGroupNotFoundException {
        try{
            lock.lock();
            checkOffset(consumerGroupId,partitionId);
            int curr = partitionOffsetPerConsumerGroup.get(consumerGroupId).get(partitionId);
            partitionOffsetPerConsumerGroup.get(consumerGroupId).put(partitionId,curr+1);
        }
        finally {
            lock.unlock();
        }
    }

    public Map<String, Map<String, Boolean>> getAllPartitionWithTopic(List<String> topicList) throws TopicNotFoundException {
        Map<String, Map<String, Boolean>> partitions= new HashMap<>();
        for(String topic: topicList){
            partitions.put(topic,new HashMap<>());
            for(String partition: clusterService.getPartitionForTopic(topic)) {
                partitions.get(topic).put(partition,true);
            }
        }
        return partitions;
    }

    public Map<String,Boolean> getPartitionForSingleTopic(String topic) throws TopicNotFoundException {
        Map<String,Boolean> partitions=new HashMap<>();
        for(String partition: clusterService.getPartitionForTopic(topic)) {
            partitions.put(partition,true);
        }
        return partitions;
    }

    public Map<String,Boolean> getPartitionForSingleTopic(String consumerGroupId, String topic) throws TopicNotFoundException {
        Map<String,Boolean> partitions=new HashMap<>();
        for(String partition: clusterService.getPartitionForTopic(topic)) {
            partitions.put(partition,true);
            partitionOffsetPerConsumerGroup.get(consumerGroupId).computeIfAbsent(partition,k->0);
        }
        return partitions;
    }
}
