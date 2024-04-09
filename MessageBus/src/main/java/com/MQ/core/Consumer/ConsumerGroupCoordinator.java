package com.MQ.core.Consumer;

import com.MQ.Config.ProducerConfig;
import com.MQ.Exception.ConsumerNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.core.OffSetManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumerGroupCoordinator {

    private static final Logger logger = LogManager.getLogger(ConsumerGroupCoordinator.class);

    private final ExecutorService executorService;
    private final String consumerGroupId;
    private final ConcurrentHashMap<String,ConcurrentHashMap<String,Boolean>> partitions;
    private final ConcurrentHashMap<Consumer,ConcurrentHashMap<String,Set<String>>> consumerToPartitionMapping;
    private final Set<String> topicList;
    private final ReentrantLock stateChangeLock = new ReentrantLock();
    private final ReentrantLock rebalancingLock = new ReentrantLock();
    private final OffSetManager offSetManager;
    private final ProducerConfig producerConfig;
    private Future rebalancedF =null;

    public ConsumerGroupCoordinator(String consumerGroupId, OffSetManager offSetManager, ProducerConfig producerConfig) {
        this.executorService = Executors.newSingleThreadExecutor();
        this.consumerGroupId = consumerGroupId;
        this.consumerToPartitionMapping=new ConcurrentHashMap<>();
        this.partitions=new ConcurrentHashMap<>();
        this.topicList=new HashSet<>();
        this.offSetManager=offSetManager;
        this.producerConfig=producerConfig;
        offSetManager.registerConsumerGroup(consumerGroupId);
    }

    public Future<?> addConsumer(Consumer consumer, List<String> topicsSubscribed) {
        try{
            stateChangeLock.lock();
            logger.info("Adding consumer "+consumer.getConsumerId()+" to consumer group "+consumerGroupId);
            consumerToPartitionMapping.computeIfAbsent(consumer,k->new ConcurrentHashMap<>());
            for(String topic: topicsSubscribed) {
                consumerToPartitionMapping.get(consumer).putIfAbsent(topic,new HashSet<>());
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

    public Future<?> removeTopicFromConsumer(Consumer consumer, List<String> topicsToUnSubscribe) throws ConsumerNotFoundException {
        try{
            stateChangeLock.lock();
            if(!consumerToPartitionMapping.containsKey(consumer))
                throw new ConsumerNotFoundException("No consumer found in group coordinator "+ consumer.getConsumerId());
            for(String topic: topicsToUnSubscribe) {
               try {
                   logger.info("Removing topic "+topic+" from consumer "+consumer.getConsumerId());
                   consumerToPartitionMapping.get(consumer).get(topic).forEach(s->{
                       partitions.get(topic).put(s,true);
                   });
                   consumerToPartitionMapping.get(consumer).remove(topic);
               }catch (Exception e) {
                   logger.error(e.getMessage());
               }
            }
        }finally {
            stateChangeLock.unlock();
        }
        return startRebalance();
    }

    public Future<?> consumerFailed(Consumer consumer) throws ConsumerNotFoundException {

        try{
            stateChangeLock.lock();

            logger.info("Consumer "+consumer.getConsumerId()+" failed ");
            if(!consumerToPartitionMapping.containsKey(consumer))
                throw new ConsumerNotFoundException("No consumer found in group coordinator "+ consumer.getConsumerId());

            for(String topic: consumerToPartitionMapping.get(consumer).keySet())
            {
               consumerToPartitionMapping.get(consumer).get(topic).forEach(s->{
                   partitions.get(topic).put(s,true);
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
            logger.info("Submitting re balancing task now");
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
            Map<String,Boolean> temp=offSetManager.getPartitionForSingleTopic(consumerGroupId,topic);
            partitions.putIfAbsent(topic,new ConcurrentHashMap<>());
            for(String partitionId: temp.keySet()) {
                logger.info("Partition "+partitionId+" added to consumer group "+consumerGroupId);
                partitions.get(topic).put(partitionId,true);
            }
        } catch (TopicNotFoundException e) {
            logger.error(e.getMessage());
        }
    }
    public void rebalanceGroup() {
        try{
            rebalancingLock.lock();
            logger.info("Re balancing initiated");
            Map<Consumer,Map<String,Integer>> desiredStateForConsumers=getDesiredState();
            for(Consumer consumer: desiredStateForConsumers.keySet()) {
                consumer.getReadyForRebalancing(desiredStateForConsumers.get(consumer));
                Map<String,List<String>> revokedPartition;

                while(true) {
                    revokedPartition = consumer.tryRevokingPartition(desiredStateForConsumers.get(consumer));
                    if(revokedPartition==null) {
                        logger.info("waiting for partitions to be revoked");
                        Thread.sleep(2000);
                    }
                    else
                        break;
                }
                for(String topic: revokedPartition.keySet()) {
                    for(String partitionId: revokedPartition.get(topic)) {
                        logger.info("partition id "+partitionId+" revoked from consumer "+consumer.getConsumerId());
                        partitions.get(topic).put(partitionId,true);
                        consumerToPartitionMapping.get(consumer).get(topic).remove(partitionId);
                    }
                }
            }
            logger.info("Revoking stage is done now moving to reallocating the partitions");

            desiredStateForConsumers.forEach(Consumer::addPartitionAsPartOfRebalancing);
            desiredStateForConsumers.keySet().forEach(Consumer::finishRebalancing);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            logger.info("Re balancing finished");
            rebalancingLock.unlock();
        }
    }

    public Map<Consumer,Map<String,Integer>> getDesiredState(){
        logger.info("Getting the desired state for all the consumer as part of rebalancing of "+consumerGroupId);
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
                logger.info("For topic "+topic+" consumer "+consumer.getConsumerId()+" should have "+total+" number of partitions");
                rem-=1;
            }
        }

        return state;
    }

    public List<String> getunassignedPartition(Consumer consumer, String topic, int tobeadded) {
        logger.info("Consumer "+consumer.getConsumerId()+" is requesting "+tobeadded+" number of partitions from topic "+topic);
        List<String> assignedPartition=new ArrayList<>();
        int currAdded=0;
        for(String partitionId: partitions.get(topic).keySet()){
            if(currAdded<tobeadded) {
                if(partitions.get(topic).get(partitionId)){
                    partitions.get(topic).put(partitionId,false);
                    logger.info(partitionId+" in consumer group "+ consumerGroupId +" to be assigned to consumer "+consumer.getConsumerId());
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
    public ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>> getPartitions() {
        return partitions;
    }

    public Future getExecutorService() {
        return rebalancedF;
    }

    public ConcurrentHashMap<Consumer, ConcurrentHashMap<String, Set<String>>> getConsumerToPartitionMapping() {
        return consumerToPartitionMapping;
    }
}
