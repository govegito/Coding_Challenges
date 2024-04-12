package com.MQ.core;

import java.util.List;
import java.util.Map;

public class BrokerPartitionBalancingStrategy implements BalancingStrategy{

    private int currCounter;
    private final int brokerNumber;

    public BrokerPartitionBalancingStrategy(int brokerNumber) {
        this.brokerNumber = brokerNumber;
        this.currCounter = 0;
    }

    public void balanceForTopic(Topic topic, List<Broker> brokerList, Map<String,Broker> brokerMap, int partitionCount)
    {
        for(int i=0;i<partitionCount;i++)
        {
            String partitionId= topic.getTopicName()+"-P"+i;
            Broker broker=brokerList.get(getNext());
            broker.addPartition(partitionId,topic);
            brokerMap.put(partitionId,broker);
        }
    }

    @Override
    public Integer getNext() {
        Integer ret= currCounter%brokerNumber;
        currCounter++;
        return ret;
    }
}
