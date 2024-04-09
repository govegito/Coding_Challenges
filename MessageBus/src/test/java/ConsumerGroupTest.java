import com.MQ.Config.ProducerConfig;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.core.ClusterService;
import com.MQ.core.Consumer.Consumer;
import com.MQ.core.Consumer.ConsumerGroupCoordinator;
import com.MQ.core.OffSetManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
@RunWith(MockitoJUnitRunner.class)
public class ConsumerGroupTest {

    @Mock
    private ClusterService service;

    @Mock
    private ProducerConfig producerConfig;

    @Mock
    private OffSetManager offSetManager;

    private Map<String,Boolean> partitionList1= new HashMap<>();
    private Map<String,Boolean> partitionList2= new HashMap<>();


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        for(int i=0;i<4;i++)
        {
            partitionList1.put("topic1-P"+i,true);
        }
        for(int i=0;i<4;i++)
        {
            partitionList2.put("topic2-P"+i,true);
        }
        try {
            when(offSetManager.getPartitionForSingleTopic("CG1","topic1")).thenReturn(this.partitionList1);
            when(offSetManager.getPartitionForSingleTopic("CG1","topic2")).thenReturn(this.partitionList2);
            when(producerConfig.getPartitionNumber()).thenReturn(4);

        } catch (TopicNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void TestAddConsumer() throws InterruptedException, ExecutionException {

        ConsumerGroupCoordinator coordinator= new ConsumerGroupCoordinator("CG1",offSetManager,producerConfig);

        Consumer consumer1 = new Consumer(coordinator,"C1",service,offSetManager);
        coordinator.addConsumer(consumer1, List.of("topic1","topic2")).get();


        ConcurrentHashMap<Consumer, ConcurrentHashMap<String,Set<String>>> consumerToPartitionMapping = coordinator.getConsumerToPartitionMapping();
        assert(consumerToPartitionMapping.get(consumer1).containsKey("topic1"));
        assert(consumerToPartitionMapping.get(consumer1).containsKey("topic2"));

        ConcurrentHashMap<String,ConcurrentHashMap<String,Boolean>> partitions= coordinator.getPartitions();
        assert(partitions.containsKey("topic1"));
        assert(partitions.containsKey("topic1"));

        assert(partitions.get("topic1").size()==4);
        assert(partitions.get("topic2").size()==4);

        Map<String, Set<String>> partitionsPerTopic = consumer1.getPartitionPerTopic();

        assert(partitionsPerTopic.containsKey("topic1"));
        assert(partitionsPerTopic.containsKey("topic2"));

        System.out.println(partitionsPerTopic.get("topic1").size());
        System.out.println(partitionsPerTopic.get("topic2").size());

        assert(partitionsPerTopic.get("topic1").size()==4);
        assert(partitionsPerTopic.get("topic2").size()==4);

    }

    @Test
    public void TestAddConsumerThenRemove() throws InterruptedException, ExecutionException {

        ConsumerGroupCoordinator coordinator= new ConsumerGroupCoordinator("CG1",offSetManager,producerConfig);

        Consumer consumer1 = new Consumer(coordinator,"C1",service,offSetManager);
        Consumer consumer2 = new Consumer(coordinator,"C2",service,offSetManager);

//        offSetManager.registerConsumerGroup("CG1");
        coordinator.addConsumer(consumer1, List.of("topic1","topic2")).get();
//        coordinator.getExecutorService().get();
        coordinator.addConsumer(consumer2, List.of("topic2")).get();


        ConcurrentHashMap<Consumer,ConcurrentHashMap<String,Set<String>>> consumerToPartitionMapping = coordinator.getConsumerToPartitionMapping();
        assert(consumerToPartitionMapping.get(consumer1).containsKey("topic1"));
        assert(consumerToPartitionMapping.get(consumer1).containsKey("topic2"));

        assert(consumerToPartitionMapping.get(consumer2).containsKey("topic2"));

        ConcurrentHashMap<String,ConcurrentHashMap<String,Boolean>> partitions= coordinator.getPartitions();
        assert(partitions.containsKey("topic1"));
        assert(partitions.containsKey("topic1"));
        System.out.println(partitions.get("topic1").size());
        assert(partitions.get("topic1").size()==4);
        assert(partitions.get("topic2").size()==4);

        Map<String, Set<String>> partitionsPerTopic = consumer1.getPartitionPerTopic();

        assert(partitionsPerTopic.containsKey("topic1"));
        assert(partitionsPerTopic.containsKey("topic2"));

        System.out.println(partitionsPerTopic.get("topic1").size());
        System.out.println(partitionsPerTopic.get("topic2").size());

        assert(partitionsPerTopic.get("topic1").size()==4);
        assert(partitionsPerTopic.get("topic2").size()==2);

        Map<String, Set<String>> partitionsPerTopic2 = consumer2.getPartitionPerTopic();

        assert(partitionsPerTopic2.containsKey("topic2"));

        System.out.println(partitionsPerTopic2.get("topic2").size()+" the last");

        assert(partitionsPerTopic2.get("topic2").size()==2);
    }
}
