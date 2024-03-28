package com.MQ.core;

import java.util.List;
import java.util.Map;

public class BrokerPartitionBalancingStrategy {

    private int currCounter;

    public BrokerPartitionBalancingStrategy() {
        this.currCounter = 0;
    }

    public void balanceForTopic(Topic topic, List<Broker> brokerList, Map<String,Broker> brokerMap, int partitionCount)
    {
        for(int i=0;i<partitionCount;i++)
        {
            String partitionId= topic.getTopicName()+"-P"+i;
            brokerList.get(currCounter%brokerList.size()).addPartition(partitionId,topic);
            brokerMap.put(partitionId,brokerList.get(currCounter%brokerList.size()));
            this.currCounter+=1;
        }
    }
}
