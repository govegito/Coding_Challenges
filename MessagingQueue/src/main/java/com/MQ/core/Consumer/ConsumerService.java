package com.MQ.core.Consumer;

import ch.qos.logback.core.util.TimeUtil;
import com.MQ.Exception.AllParitionOccupied;
import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.ConsumerNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.Models.ConsumerMessagePayload;
import com.MQ.core.ClusterService;
import com.MQ.core.OffSetManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ConsumerService {

    private static final Logger logger = LogManager.getLogger(ConsumerService.class);

    @Autowired
    private ConsumerGroupService consumerGroupService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private OffSetManager offSetManager;

    private volatile Map<String,Consumer> consumerMap;
    private volatile Map<Consumer, Long> heartbeatTime;

    private ReentrantLock lock = new ReentrantLock();

    public ConsumerService() {
        consumerMap=new HashMap<>();
        heartbeatTime=new HashMap<>();
    }

    public void registerConsumer(String consumerId, ConsumerGroupCoordinator groupCoordinator) throws Exception {
        if(!consumerMap.containsKey(consumerId)) {
            Consumer consumer = new Consumer(groupCoordinator,consumerId,clusterService,offSetManager);
            consumerMap.put(consumerId,consumer);
            heartbeatTime.put(consumer,System.currentTimeMillis());

        }
        else
            throw new Exception("Consumer already exist");
    }

    public void subscribeToTopics(String consumerId, List<String> topicList) throws ConsumerNotFoundException, ConsumerGroupNotFoundException, TopicNotFoundException {
        Consumer consumer =getConsumer(consumerId);
        heartbeatTime.put(consumer,System.currentTimeMillis());
        consumerGroupService.addConsumerToConsumerGroup(consumer.getGroupCoordinator().getConsumerGroupId(),consumer,topicList);
    }

    public void unsubscribeToTopics(String consumerId, List<String> topicList) throws ConsumerNotFoundException, ConsumerGroupNotFoundException, TopicNotFoundException {
        Consumer consumer = getConsumer(consumerId);
        heartbeatTime.put(consumer,System.currentTimeMillis());
        consumerGroupService.removeTopicFromConsumer(consumer.getGroupCoordinator().getConsumerGroupId(),consumer,topicList);
    }

    public void removeConsumer(Consumer consumer) throws ConsumerGroupNotFoundException, ConsumerNotFoundException {
        consumerMap.remove(consumer.getConsumerId());
        heartbeatTime.remove(consumer);
        consumerGroupService.removeConsumerFromConsumerGroup(consumer.getGroupCoordinator().getConsumerGroupId(),consumer);
    }

    public void removeConsumer(String consumerId) throws ConsumerGroupNotFoundException, ConsumerNotFoundException {
        Consumer consumer= getConsumer(consumerId);
        removeConsumer(consumer);
    }

    public ConsumerMessagePayload poll(String consumerId) throws ConsumerNotFoundException, AllParitionOccupied, ConsumerGroupNotFoundException {
        Consumer consumer = getConsumer(consumerId);
        heartbeatTime.put(consumer,System.currentTimeMillis());
        return consumer.poll();
    }

    public void acknowledgementReceived(String consumerId, Long ack) throws ConsumerNotFoundException {
        Consumer consumer = getConsumer(consumerId);
        heartbeatTime.put(consumer,System.currentTimeMillis());
        consumer.acknowledgementReceived(ack);
    }

    public void heartBeatRecieved(String consumerId) throws ConsumerNotFoundException {
        Consumer consumer = getConsumer(consumerId);
        heartbeatTime.put(consumer,System.currentTimeMillis());
    }

    private Consumer getConsumer(String consumerId) throws ConsumerNotFoundException {
        if(!consumerMap.containsKey(consumerId))
            throw new ConsumerNotFoundException("consumer id "+consumerId+" not found for subscribing ");
        return consumerMap.get(consumerId);
    }

    @Scheduled(fixedRate = 60000)
    private void runHealthCheck(){

        List<Consumer> toRemove=new ArrayList<>();
        for(Consumer consumer: heartbeatTime.keySet())
        {
            if((System.currentTimeMillis()-heartbeatTime.get(consumer))>=60000)
                toRemove.add(consumer);
        }

        for(Consumer consumer: toRemove)
        {
            try {
                removeConsumer(consumer);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
