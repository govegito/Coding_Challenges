package com.MQ.core.Consumer;

import com.MQ.Exception.AllParitionOccupied;
import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.PartitionIsEmptyException;
import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Models.ConsumerMessagePayload;
import com.MQ.core.ClusterService;
import com.MQ.core.OffSetManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Consumer {
    private static final Logger logger = LogManager.getLogger(Consumer.class);
    private ClusterService clusterService;
    private OffSetManager offsetManager;
    private final ConsumerGroupCoordinator groupCoordinator;
    private final String consumerId;
    private  AtomicLong currPartition;
    private  ConcurrentHashMap<String,Boolean> isTopicBeingRevoked;
    private  ConcurrentHashMap<String, Set<String>> partitionPerTopic;
    private  List<String> availablePartition;
    private  ConcurrentHashMap<String,Long> inProcessPartition;
    private ReentrantLock pollLock = new ReentrantLock();

    public Consumer(ConsumerGroupCoordinator groupCoordinator, String consumerId, ClusterService clusterService, OffSetManager offSetManager) {
        this.groupCoordinator = groupCoordinator;
        this.currPartition=new AtomicLong(0);
        this.consumerId = consumerId;
        this.partitionPerTopic = new ConcurrentHashMap<>();
        this.inProcessPartition=new ConcurrentHashMap<>();
        this.availablePartition=Collections.synchronizedList(new ArrayList<>());
        this.isTopicBeingRevoked=new ConcurrentHashMap<>();
        this.clusterService=clusterService;
        this.offsetManager=offSetManager;//        this.partitionToTopic=new HashMap<>();
    }

    private String getTopicFromPartition(String partitionId) {
        for(String topic: partitionPerTopic.keySet()) {
            if(partitionPerTopic.get(topic).contains(partitionId))
                return topic;
        }
        return null;
    }

    public ConsumerMessagePayload poll() throws AllParitionOccupied, ConsumerGroupNotFoundException {
        try{
            pollLock.lock();
            String partitionId;
            for(int i=0;i<availablePartition.size();i++) {
                partitionId= availablePartition.get((int)currPartition.incrementAndGet()%availablePartition.size());
                String topic=getTopicFromPartition(partitionId);
                logger.info("Checking partitionId "+partitionId+" for consumer "+consumerId+" having in process id"+inProcessPartition.get(partitionId));
                if(!(inProcessPartition.get(partitionId)>=0 || isTopicBeingRevoked.getOrDefault(topic,false))) {
                    try {
                        int offset = offsetManager.getOffset(groupCoordinator.getConsumerGroupId(),partitionId);
                        ConsumerMessagePayload payload=new ConsumerMessagePayload(clusterService.getMessageFromPartition(partitionId,offset),currPartition.get());
                        inProcessPartition.put(partitionId,currPartition.get());
                        logger.info("Returning payload from partition "+partitionId+" with message sequence id "+payload.getMessageId());
                        return payload;
                    } catch (PartitionNotFoundException | PartitionIsEmptyException e) {
                        logger.error(e.getMessage()+" for the consumer "+consumerId);
                    }
                }
            }
            logger.info("No more partition left to process concurrently");
            throw new AllParitionOccupied("No more partition left to process concurrently for consumer "+consumerId);
        }finally {
            pollLock.unlock();
        }
    }

    private String getPartitionForAcknowledgement(long messageId) {
        for(String partitionId: inProcessPartition.keySet()) {
            if(inProcessPartition.get(partitionId)==messageId)
                return partitionId;
        }
        return null;
    }
    public void acknowledgementReceived(long messageid) {
        try{
            pollLock.lock();
            String partitionId=getPartitionForAcknowledgement(messageid);

            if(partitionId==null || partitionId.equalsIgnoreCase(""))
                return;
            inProcessPartition.put(partitionId, (long) -1);
            logger.info("Acknowledging message processed for partition "+partitionId+" with sequence number "+messageid+" in consumer "+consumerId);
            offsetManager.commitOffset(groupCoordinator.getConsumerGroupId(),partitionId);

        } catch (PartitionNotFoundException | ConsumerGroupNotFoundException e) {
            logger.info(e.getMessage()+" for consumer "+consumerId);
            throw new RuntimeException(e);
        } finally {
            pollLock.unlock();
        }
    }

    public void getReadyForRebalancing(Map<String,Integer> desiredState) {
        try{
            pollLock.lock();

            for(String topic: partitionPerTopic.keySet()) {
                if(desiredState.getOrDefault(topic,0)<partitionPerTopic.get(topic).size()) {
                    isTopicBeingRevoked.put(topic,true);
                    logger.info("Putting lock on polling for topic"+topic+" in consumer "+consumerId);
                }
            }
        }finally {
            pollLock.unlock();
        }
    }

    public Map<String,List<String>> tryRevokingPartition(Map<String,Integer> desiredState) {

        Map<String,Integer> currState=new HashMap<>();
        String currTopic;

        for(String partitionId: inProcessPartition.keySet()) {
            logger.info("Checking if the partition is ready to be revoked "+partitionId+"  in the consumer "+consumerId+" ");
            if(inProcessPartition.get(partitionId)>=0)
                continue;
            currTopic=getTopicFromPartition(partitionId);
            currState.put(currTopic,currState.getOrDefault(currTopic,0)+1);
        }

        for(String topic: desiredState.keySet()) {
            int toBeRevoked = -1;
            if(partitionPerTopic.containsKey(topic))
                toBeRevoked= partitionPerTopic.get(topic).size()-desiredState.getOrDefault(topic,0);
            logger.info("For topic "+topic+" to be revoked partition count is "+toBeRevoked+" with currently having unoccupied partition count "+currState.getOrDefault(topic,0)+" for consumer "+consumerId);
            if(toBeRevoked>currState.getOrDefault(topic,0))
                return null;
        }

        Map<String,List<String>> evokedPartition= new HashMap<>();
        pollLock.lock();
        logger.info("Consumer "+consumerId+" preparing to revoke partitions");
        for(String topic: desiredState.keySet()) {
            evokedPartition.put(topic,new ArrayList<>());
            int toBeRevoked = -1;

            if(partitionPerTopic.containsKey(topic))
                toBeRevoked= partitionPerTopic.get(topic).size()-desiredState.getOrDefault(topic,0);

            if(toBeRevoked<=0)
                continue;

            logger.info("so in consumer "+consumerId+" evoking "+toBeRevoked+" for topic "+topic);

            List<String> revokedPartitions = getUnoccupiedPartitions(topic).subList(0, toBeRevoked);

            for(String partitionId: revokedPartitions) {
                availablePartition.remove(partitionId);
                partitionPerTopic.get(topic).remove(partitionId);
                inProcessPartition.remove(partitionId);
                evokedPartition.get(topic).add(partitionId);
            }
        }
        pollLock.unlock();

        return evokedPartition;
    }

    private List<String> getUnoccupiedPartitions(String topic) {
        List<String> partitionList=new ArrayList<>();
        for(String partitionId: partitionPerTopic.get(topic).stream().toList()) {
            if(inProcessPartition.get(partitionId)<0)
                partitionList.add(partitionId);
        }
        return partitionList;
    }


    public void addPartitionAsPartOfRebalancing(Map<String,Integer> desiredState) {
        try{
            pollLock.lock();
            for(String topic: desiredState.keySet()) {
                int tobeadded;
                if(partitionPerTopic.containsKey(topic)) {
                    tobeadded=desiredState.get(topic)-partitionPerTopic.get(topic).size();
                }
                else {
                    tobeadded=desiredState.get(topic);
                    partitionPerTopic.computeIfAbsent(topic,k->new HashSet<>());
                }
                if(tobeadded<=0)
                    continue;

                List<String> allocatedPartition=groupCoordinator.getunassignedPartition(this,topic,tobeadded);

                for(String partitionId: allocatedPartition) {
                    logger.info(partitionId+" for consumer "+consumerId);
                    partitionPerTopic.get(topic).add(partitionId);
                    availablePartition.add(partitionId);
                    inProcessPartition.put(partitionId, (long) -1);
                }
            }
        }finally {
            pollLock.unlock();
        }
    }

    public void finishRebalancing() {
        for(String topic: isTopicBeingRevoked.keySet()) {
            isTopicBeingRevoked.put(topic,false);
        }
    }

    public String getConsumerId() {
        return consumerId;
    }

    public Map<String, Set<String>> getPartitionPerTopic() {
        return partitionPerTopic;
    }

    public ConsumerGroupCoordinator getGroupCoordinator() {
        return groupCoordinator;
    }
}
