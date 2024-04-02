package com.MQ.core.Consumer;

import com.MQ.Exception.AllParitionOccupied;
import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.PartitionIsEmptyException;
import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Models.ConsumerMessagePayload;
import com.MQ.core.ClusterService;
import com.MQ.core.OffSetManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class Consumer {
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private OffSetManager offsetManager;
    private final ConsumerGroupCoordinator groupCoordinator;
    private final String consumerId;
    private volatile long currPartition;
    private volatile Map<String,Boolean> isTopicBeingRevoked;
    private volatile  Map<String, Set<String>> partitionPerTopic;
    private volatile  List<String> availablePartition;
    private volatile Map<String,Long> inProcessPartition;
    private ReentrantLock pollLock = new ReentrantLock();

    public Consumer(ConsumerGroupCoordinator groupCoordinator, String consumerId) {
        this.groupCoordinator = groupCoordinator;
        this.currPartition=0L;
        this.consumerId = consumerId;
        this.partitionPerTopic = new HashMap<>();
        this.inProcessPartition=new HashMap<>();
        this.availablePartition=new ArrayList<>();
        this.isTopicBeingRevoked=new HashMap<>();
//        this.partitionToTopic=new HashMap<>();
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

                partitionId= availablePartition.get((int)currPartition%availablePartition.size());
                String topic=getTopicFromPartition(partitionId);

                if(!(inProcessPartition.getOrDefault(partitionId,null)!=null || isTopicBeingRevoked.getOrDefault(topic,false))) {
                    inProcessPartition.put(partitionId,currPartition);
                    try {
                        int offset = offsetManager.getOffset(groupCoordinator.getConsumerGroupId(),partitionId);
                        return new ConsumerMessagePayload(clusterService.getMessageFromPartition(partitionId,offset),currPartition);
                    } catch (PartitionNotFoundException | PartitionIsEmptyException e) {
                        System.out.println(e.getMessage());
                    }
                }
                currPartition+=1;
            }

            throw new AllParitionOccupied("No more partition left to process concurrently");
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

            if(partitionId!=null && partitionId.equalsIgnoreCase(""))
                return;
            inProcessPartition.put(partitionId,null);
            offsetManager.commitOffset(groupCoordinator.getConsumerGroupId(),partitionId);

        } catch (PartitionNotFoundException | ConsumerGroupNotFoundException e) {
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
            if(inProcessPartition.get(partitionId)!=null)
                continue;

            currTopic=getTopicFromPartition(partitionId);
            currState.put(currTopic,currState.getOrDefault(currTopic,0)+1);
        }

        for(String topic: desiredState.keySet()) {
            int toBeRevoked = -1;
            if(partitionPerTopic.containsKey(topic))
                toBeRevoked= partitionPerTopic.get(topic).size()-desiredState.getOrDefault(topic,0);
            if(toBeRevoked>currState.getOrDefault(topic,0))
                return null;
        }

        Map<String,List<String>> evokedPartition= new HashMap<>();

        for(String topic: desiredState.keySet()) {
            evokedPartition.put(topic,new ArrayList<>());
            int toBeRevoked = -1;

            if(partitionPerTopic.containsKey(topic))
                toBeRevoked= partitionPerTopic.get(topic).size()-desiredState.getOrDefault(topic,0);

            if(toBeRevoked<=0)
                continue;

            System.out.println("so in consumer "+consumerId+" evoking "+toBeRevoked+" for topic "+topic);

            List<String> revokedPartitions = getUnoccupiedPartitions(topic).subList(0, toBeRevoked);

            for(String partitionId: revokedPartitions) {
                availablePartition.remove(partitionId);
                partitionPerTopic.get(topic).remove(partitionId);
                inProcessPartition.remove(partitionId);
                evokedPartition.get(topic).add(partitionId);
            }
        }

        return evokedPartition;
    }

    private List<String> getUnoccupiedPartitions(String topic) {
        List<String> partitionList=new ArrayList<>();
        for(String partitionId: partitionPerTopic.get(topic).stream().toList()) {
            if(inProcessPartition.getOrDefault(partitionId,null)==null)
                partitionList.add(partitionId);
        }
        return partitionList;
    }


    public void addPartitionAsPartOfRebalancing(Map<String,Integer> desiredState) {
        for(String topic: desiredState.keySet()) {
            int tobeadded;
            if(partitionPerTopic.containsKey(topic)) {
                tobeadded=desiredState.get(topic)-partitionPerTopic.get(topic).size();
            }
            else {
                tobeadded=desiredState.get(topic);
                partitionPerTopic.computeIfAbsent(topic,k->new HashSet<>());
            }
            System.out.println(topic+" Are yrrr partition kha h for "+consumerId+" "+tobeadded);

            if(tobeadded<=0)
                continue;

            List<String> allocatedPartition=groupCoordinator.getunassignedPartition(this,topic,tobeadded);

            for(String partitionId: allocatedPartition) {
                System.out.println(partitionId+" for consumer "+consumerId);
                partitionPerTopic.get(topic).add(partitionId);
                availablePartition.add(partitionId);
                inProcessPartition.put(partitionId,null);

            }
        }
    }

    public void finishRebalancing() {
        for(String topic: isTopicBeingRevoked.keySet()) {
            isTopicBeingRevoked.put(topic,false);
        }
        this.currPartition=0L;
        this.inProcessPartition=new HashMap<>();

    }

    public String getConsumerId() {
        return consumerId;
    }

    public Map<String, Set<String>> getPartitionPerTopic() {
        return partitionPerTopic;
    }
}
