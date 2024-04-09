package com.MQ.core.Consumer;

import com.MQ.Config.ProducerConfig;
import com.MQ.Controllers.ConsumerGroupController;
import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.ConsumerNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.core.ClusterService;
import com.MQ.core.OffSetManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class ConsumerGroupService {

    private static final Logger logger = LogManager.getLogger(ConsumerGroupService.class);

    @Autowired
    private ClusterService clusterService;
    @Autowired
    private OffSetManager offSetManager;
    @Autowired
    private ProducerConfig producerConfig;
    private Map<String,ConsumerGroupCoordinator> coordinatorMap;

    public ConsumerGroupService() {
        this.coordinatorMap = new HashMap<>();
    }

    public void addConsumerGroup(String consumerGroupId) {
        if(!coordinatorMap.containsKey(consumerGroupId)) {
            coordinatorMap.put(consumerGroupId,new ConsumerGroupCoordinator(consumerGroupId,offSetManager,producerConfig));
        }
    }

    public ConsumerGroupCoordinator getConsumerGroup(String consumerGroupId) throws ConsumerGroupNotFoundException {
        if(!coordinatorMap.containsKey(consumerGroupId))
            throw new ConsumerGroupNotFoundException("No consumer group found for id "+consumerGroupId);
        return coordinatorMap.get(consumerGroupId);

    }
    public void removeConsumerGroup(String consumerGroupId) throws ConsumerGroupNotFoundException {
        if(!coordinatorMap.containsKey(consumerGroupId))
            throw new ConsumerGroupNotFoundException("No consumer group found for id "+consumerGroupId);

        coordinatorMap.remove(consumerGroupId);
    }

    public void addConsumerToConsumerGroup(String consumerGroupId, Consumer consumer, List<String> topicList) throws TopicNotFoundException, ConsumerGroupNotFoundException {

        ConsumerGroupCoordinator groupCoordinator = getConsumerGroup(consumerGroupId);
        for(String topic: topicList) {
            if(!clusterService.ifTopicPresent(topic))
                throw new TopicNotFoundException("No topic found to add to the consumer "+consumer.getConsumerId()+" with topic name "+topic);
        }
        groupCoordinator.addConsumer(consumer,topicList);
    }

    public void removeConsumerFromConsumerGroup(String consumerGroupId, Consumer consumer) throws ConsumerGroupNotFoundException, ConsumerNotFoundException {
        ConsumerGroupCoordinator groupCoordinator = getConsumerGroup(consumerGroupId);
        groupCoordinator.consumerFailed(consumer);
    }

    public void removeTopicFromConsumer(String consumerGroupId, Consumer consumer, List<String> topicList) throws ConsumerGroupNotFoundException, TopicNotFoundException, ConsumerNotFoundException {

        ConsumerGroupCoordinator groupCoordinator = getConsumerGroup(consumerGroupId);
        for(String topic: topicList) {
            if(!clusterService.ifTopicPresent(topic))
                throw new TopicNotFoundException("No topic found to add to the consumer "+consumer.getConsumerId()+" with topic name "+topic);
        }
        groupCoordinator.removeTopicFromConsumer(consumer,topicList);
    }


}
