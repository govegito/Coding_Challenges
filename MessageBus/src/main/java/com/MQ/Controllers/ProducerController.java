package com.MQ.Controllers;

import com.MQ.Exception.PartitionNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.Models.Message;
import com.MQ.Models.MessagePayload;
import com.MQ.Models.ProducerAcknowledgement;
import com.MQ.core.ClusterService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Controller
public class ProducerController {

    private static final Logger logger = LogManager.getLogger(ProducerController.class);
    @Autowired
    private SimpMessagingTemplate simpMessagingTemplate;

    private ExecutorService executorService;
    @Autowired
    private ClusterService service;

    @PostConstruct
    public void init()
    {
        this.executorService = Executors.newFixedThreadPool(50);

    }

    @MessageMapping("/publish/{topic}")
    public void greeting(@DestinationVariable String topic, MessagePayload message) throws Exception {
        logger.info("Publish message call received "+topic);
        executorService.execute(() -> {
            try {
                // Process the message
                Message messageToPublish = new Message(message, topic);
                service.writeMessageToTopic(messageToPublish);
                // Send acknowledgment
                ProducerAcknowledgement ack = new ProducerAcknowledgement(message.getMessageID(), "ACK");
                simpMessagingTemplate.convertAndSend("/topic/acknowledgement/" + topic, ack);
            } catch (Exception e) {
                // Handling exceptions
                e.printStackTrace();
                // Send error acknowledgment
                ProducerAcknowledgement ack = new ProducerAcknowledgement(message.getMessageID(), "FAILED");
                simpMessagingTemplate.convertAndSend("/topic/acknowledgement/" + topic, ack);
            }
        });
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdown();
    }
}
