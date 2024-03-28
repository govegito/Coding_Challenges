package com.MQ.core.Consumer;

import com.MQ.Exception.AllParitionOccupied;
import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.PartitionIsEmptyException;
import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Models.ConsumerMessagePayload;
import com.MQ.core.ClusterService;
import com.MQ.core.OffSetManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Consumer {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private OffSetManager offsetManager;

    private final ConsumerGroupCoordinator groupCoordinator;
    private final String consumerId;

    private volatile long currPartition;

    private volatile Map<String,String> partitionToTopic;
    private volatile Map<String,Boolean> isTopicBeingRevoked;
    private volatile  Map<String, List<String>> partitionPerTopic;
    private volatile  List<String> availablePartition;
    private volatile  Map<String,Boolean> partitionOccupied;
    private volatile Map<Long,String> inProcessPartition;
    private ReentrantLock pollLock = new ReentrantLock();

    public Consumer(ConsumerGroupCoordinator groupCoordinator, String consumerId) {
        this.groupCoordinator = groupCoordinator;
        this.currPartition=0L;
        this.consumerId = consumerId;
        this.partitionPerTopic = new HashMap<>();
        this.partitionOccupied = new HashMap<>();
        this.inProcessPartition=new HashMap<>();
        this.availablePartition=new ArrayList<>();
        this.isTopicBeingRevoked=new HashMap<>();
        this.partitionToTopic=new HashMap<>();
    }

    public ConsumerMessagePayload poll() throws AllParitionOccupied, ConsumerGroupNotFoundException {
        try{
            pollLock.lock();
            String partitionId;
            for(int i=0;i<availablePartition.size();i++)
            {
                partitionId= availablePartition.get((int)currPartition%availablePartition.size());
                if(!(partitionOccupied.getOrDefault(partitionId,false) || isTopicBeingRevoked.getOrDefault(partitionToTopic.get(partitionId),false)))
                {
                    partitionOccupied.put(partitionId,true);
                    inProcessPartition.put(currPartition,partitionId);

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

    public void acknowledgementReceived(long messageid)
    {
        try{
            pollLock.lock();
            String partitionId=inProcessPartition.getOrDefault(messageid,"");
            inProcessPartition.remove(messageid);

            if(partitionId.equalsIgnoreCase(""))
                return;

            partitionOccupied.put(partitionId,false);
            offsetManager.checkOffset(groupCoordinator.getConsumerGroupId(),partitionId);

        } catch (PartitionNotFoundException e) {
            throw new RuntimeException(e);
        } catch (ConsumerGroupNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            pollLock.unlock();
        }
    }

    public void getReadyForRebalancing(Map<String,Integer> desiredState)
    {
        try{
            pollLock.lock();

            for(String topic: partitionPerTopic.keySet())
            {
                if(desiredState.getOrDefault(topic,0)<partitionPerTopic.get(topic).size())
                {
                    isTopicBeingRevoked.put(topic,true);
                }
            }
        }finally {
            pollLock.unlock();
        }
    }

    public Map<String,List<String>> tryRevokingPartition(Map<String,Integer> desiredState)
    {
        Map<String,Integer> currState=new HashMap<>();
        String currTopic;
        for(String partitionId: partitionOccupied.keySet())
        {
            if(partitionOccupied.get(partitionId))
                continue;

            currTopic=partitionToTopic.get(partitionId);

            currState.put(currTopic,currState.getOrDefault(currTopic,0)+1);
        }

        for(String topic: desiredState.keySet())
        {
            int toBeRevoked = -1;
            if(partitionPerTopic.containsKey(topic))
                toBeRevoked= partitionPerTopic.get(topic).size()-desiredState.getOrDefault(topic,0);
            if(toBeRevoked>currState.getOrDefault(topic,0))
            {
                return null;
            }
        }

        Map<String,List<String>> evokedPartition= new HashMap<>();

        for(String topic: desiredState.keySet())
        {
            evokedPartition.put(topic,new ArrayList<>());
            int toBeRevoked = -1;
            if(partitionPerTopic.containsKey(topic))
                toBeRevoked= partitionPerTopic.get(topic).size()-desiredState.getOrDefault(topic,0);
            System.out.println("so in consumer "+consumerId+" evoking "+toBeRevoked+" for topic "+topic);

            if(toBeRevoked<=0)
                continue;
            List<String> revokedPartitions = partitionPerTopic.get(topic).subList(0, toBeRevoked);

            for(String partionId: revokedPartitions)
            {
                partitionToTopic.remove(partionId);
                availablePartition.remove(partionId);
                partitionOccupied.remove(partionId);
                evokedPartition.get(topic).add(partionId);
            }

            partitionPerTopic.get(topic).removeAll(revokedPartitions);
        }

        return evokedPartition;
    }


    public void addPartitionAsPartOfRebalancing(Map<String,Integer> desiredState)
    {
        for(String topic: desiredState.keySet())
        {
            int tobeadded;
            if(partitionPerTopic.containsKey(topic))
            {
                tobeadded=desiredState.get(topic)-partitionPerTopic.get(topic).size();
            }
            else
            {
                tobeadded=desiredState.get(topic);
                partitionPerTopic.put(topic,new ArrayList<>());
            }
            System.out.println(topic+" Are yrrr partition kha h for "+consumerId+" "+tobeadded);

            if(tobeadded<=0)
                continue;

            List<String> allocatedPartition=groupCoordinator.getunassignedPartition(consumerId,topic,tobeadded);

            for(String partitionId: allocatedPartition)
            {
                System.out.println(partitionId+" for consumer "+consumerId);
                partitionPerTopic.get(topic).add(partitionId);
                availablePartition.add(partitionId);
                partitionOccupied.put(partitionId,false);
                partitionToTopic.put(partitionId,topic);
            }
        }
    }

    public void finishRebalancing()
    {
        for(String topic: isTopicBeingRevoked.keySet())
        {
            isTopicBeingRevoked.put(topic,false);
        }

        this.currPartition=0L;
        this.inProcessPartition=new HashMap<>();

    }

    public String getConsumerId() {
        return consumerId;
    }

    public Map<String, List<String>> getPartitionPerTopic() {
        return partitionPerTopic;
    }
}
