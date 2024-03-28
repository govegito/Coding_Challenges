package com.MQ.core.Consumer;

import com.MQ.Config.ProducerConfig;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.core.OffSetManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumerGroupCoordinator {

    private volatile ExecutorService executorService;
    private final String consumerGroupId;
    private volatile Map<String,Map<String,Boolean>> partitions;
    private volatile Map<Consumer,Map<String,Set<String>>> consumerToPartitionMapping;
    private volatile Set<String> topicList;
    private ReentrantLock stateChangeLock = new ReentrantLock();
    private ReentrantLock rebalancingLock = new ReentrantLock();

    @Autowired
    private OffSetManager offSetManager;
    @Autowired
    private ProducerConfig producerConfig;

    private Future rebalanceF=null;

    public ConsumerGroupCoordinator(String consumerGroupId) {
        this.executorService = Executors.newSingleThreadExecutor();
        this.consumerGroupId = consumerGroupId;
        this.consumerToPartitionMapping=new HashMap<>();
        this.partitions=new HashMap<>();
        this.topicList=new HashSet<>();
    }

    public Future<?> addConsumer(Consumer consumer, List<String> topicsSubscribed) {
        try{
            stateChangeLock.lock();
            consumerToPartitionMapping.computeIfAbsent(consumer,k->new HashMap<>());
            for(String topic: topicsSubscribed) {
                consumerToPartitionMapping.get(consumer).computeIfAbsent(topic,k-> new HashSet<>());
                if(!topicList.contains(topic)) {
                    addPartitions(topic);
                    topicList.add(topic);
                }
            }
        }finally {
            stateChangeLock.unlock();
        }
        return startRebalance();
    }

    public Future<?> removeTopicFromConsumer(Consumer consumer, List<String> topicsToUnSubscribe) {
        try{
            stateChangeLock.lock();
            for(String topic: topicsToUnSubscribe) {
               try {
                   consumerToPartitionMapping.get(consumer).get(topic).forEach(s->{
                       partitions.get(topic).put(s,false);
                   });
                   consumerToPartitionMapping.get(consumer).remove(topic);
               }catch (NullPointerException e) {
                   System.out.println(e.getMessage());
               }
            }
        }finally {
            stateChangeLock.unlock();
        }
        return startRebalance();
    }

    public Future<?> consumerFailed(Consumer consumer) {
        try{
            stateChangeLock.lock();
            for(String topic: consumerToPartitionMapping.get(consumer).keySet())
            {
               consumerToPartitionMapping.get(consumer).get(topic).forEach(s->{
                   partitions.get(topic).put(s,false);
               });
            }
            consumerToPartitionMapping.remove(consumer);
        }finally {
            stateChangeLock.unlock();
        }
        return startRebalance();
    }

    public Future<?> startRebalance(){
        try{
            rebalancingLock.lock();
            return executorService.submit(()->{
                try{
                    rebalanceGroup();
                }catch (Exception e)
                {
                    Thread.currentThread().interrupt();
                }
            });
        }finally {
            rebalancingLock.unlock();
        }
    }

    public void addPartitions(String topic) {
        try {
            partitions.put(topic,offSetManager.getPartitionForSingleTopic(topic));
        } catch (TopicNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public void rebalanceGroup() {
        try{
            rebalancingLock.lock();
            Map<Consumer,Map<String,Integer>> desiredStateForConsumers=getDesiredState();
            for(Consumer consumer: desiredStateForConsumers.keySet()) {
                consumer.getReadyForRebalancing(desiredStateForConsumers.get(consumer));
                Map<String,List<String>> revokedPartition;

                while(true) {
                    revokedPartition = consumer.tryRevokingPartition(desiredStateForConsumers.get(consumer));
                    if(revokedPartition==null)
                        Thread.sleep(2000);
                    else
                        break;
                }
                for(String topic: revokedPartition.keySet()) {
                    for(String partitionId: revokedPartition.get(topic)) {
                        partitions.get(topic).put(partitionId,true);
                        consumerToPartitionMapping.get(consumer.getConsumerId()).get(topic).remove(partitionId);
                    }
                }
            }

            desiredStateForConsumers.forEach(Consumer::addPartitionAsPartOfRebalancing);
            desiredStateForConsumers.keySet().forEach(Consumer::finishRebalancing);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            rebalancingLock.unlock();
        }
    }

    public Map<Consumer,Map<String,Integer>> getDesiredState(){

        Map<Consumer,Map<String,Integer>> state = new HashMap<>();

        for(String topic: topicList) {
            int numberOfPartition= producerConfig.getPartitionNumber();

            List<Consumer> consumerList= new ArrayList<>();

            consumerToPartitionMapping.keySet().forEach(consumer->{
                if(consumerToPartitionMapping.get(consumer).containsKey(topic))
                    consumerList.add(consumer);
            });

            if(consumerList.isEmpty())
                continue;

            int rem=numberOfPartition%consumerList.size();

            for(Consumer consumer: consumerList) {
                if(!state.containsKey(consumer))
                    state.put(consumer,new HashMap<>());

                int total=numberOfPartition/consumerList.size() + (rem>0?1:0);
                state.get(consumer).put(topic,total);
                rem-=1;
            }
        }

        return state;
    }

    public List<String> getunassignedPartition(Consumer consumer, String topic, int tobeadded) {

        List<String> assignedPartition=new ArrayList<>();
        int currAdded=0;
        for(String partitionId: partitions.get(topic).keySet()){
            if(currAdded<tobeadded) {
                if(partitions.get(topic).get(partitionId)){
                    partitions.get(topic).put(partitionId,false);
                    System.out.println(partitionId+"in consumer group to be assigned to consumer "+consumer.getConsumerId());
                    consumerToPartitionMapping.get(consumer).get(topic).add(partitionId);
                    assignedPartition.add(partitionId);
                    currAdded+=1;
                }
            }
            else
                break;
        }
        return assignedPartition;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }
    public Map<String, Map<String, Boolean>> getPartitions() {
        return partitions;
    }

    public Future getExecutorService() {
        return rebalanceF;
    }

    public Map<Consumer, Map<String, Set<String>>> getConsumerToPartitionMapping() {
        return consumerToPartitionMapping;
    }
}
