package com.MQ.Controllers;

import com.MQ.Exception.AllParitionOccupied;
import com.MQ.Exception.ConsumerGroupNotFoundException;
import com.MQ.Exception.ConsumerNotFoundException;
import com.MQ.Exception.TopicNotFoundException;
import com.MQ.Models.ConsumerMessagePayload;
import com.MQ.core.Consumer.ConsumerGroupCoordinator;
import com.MQ.core.Consumer.ConsumerGroupService;
import com.MQ.core.Consumer.ConsumerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {

    private static final Logger logger = LogManager.getLogger(ConsumerController.class);

    @Autowired
    private ConsumerGroupService consumerGroupService;

    @Autowired
    private ConsumerService consumerService;

    @RequestMapping(value = "/register-consumer", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> registerConsumer(@RequestBody Map<String,String> request) {
        String groupName=request.get("consumer_group_name");
        String consumerName=request.get("consumer_name");
        logger.info("register consumer call received for consumer name "+consumerName+"  and consumer group "+groupName );
        try{
            ConsumerGroupCoordinator groupCoordinator = consumerGroupService.getConsumerGroup(groupName);
            consumerService.registerConsumer(consumerName,groupCoordinator);
            return ResponseEntity.status(HttpStatus.OK).body("Consumer group added");
        }catch (ConsumerGroupNotFoundException e) {
            logger.error(e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage()+" try to first register the consumer group");
        } catch (Exception e) {
            logger.error(e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        }
    }

    @RequestMapping(value = "/subscribe-topic", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> subscribeToTopic(@RequestBody Map<String,String> request) {
        String consumerName=request.get("consumer_name");
        List<String> topicList = Arrays.stream(request.get("topic_list").split(",")).toList();
        logger.info("Subscribe to topic call received for consumer name "+consumerName+"  and topic list "+ request.get("topic_list"));
        try{
            consumerService.subscribeToTopics(consumerName,topicList);
            return ResponseEntity.status(HttpStatus.OK).body("Consumer group added");
        } catch (ConsumerGroupNotFoundException | ConsumerNotFoundException | TopicNotFoundException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage()+" please try to re evaluate the request parameters");
        }
    }

    @RequestMapping(value = "/unsubscribe-topic", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> unsubscribeToTopic(@RequestBody Map<String,String> request) {
        String consumerName=request.get("consumer_name");
        List<String> topicList = Arrays.stream(request.get("topic_list").split(",")).toList();
        logger.info("Unsubscribe to topic call received for consumer name "+consumerName+"  and topic list "+ request.get("topic_list"));
        try{
            consumerService.unsubscribeToTopics(consumerName,topicList);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Consumer group added");
        } catch (ConsumerGroupNotFoundException | ConsumerNotFoundException | TopicNotFoundException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage()+" please try to re evaluate the request parameters");
        }
    }

    @RequestMapping(value = "/poll", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> poll(@RequestBody Map<String,String> request) {
        String consumerName=request.get("consumer_name");
        logger.info("Polling call received for consumer name "+consumerName);
        try{
            ConsumerMessagePayload messagePayload=consumerService.poll(consumerName);
            logger.info("now passing messgae with message id"+messagePayload.getMessageId());
            return ResponseEntity.status(HttpStatus.OK).body(messagePayload);
        } catch (ConsumerGroupNotFoundException | ConsumerNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage()+" please try to re evaluate the request parameters");
        } catch (AllParitionOccupied e) {
            return ResponseEntity.status(HttpStatus.BANDWIDTH_LIMIT_EXCEEDED).body(e.getMessage()+" please try acknowledging completion of in process messages");
        }
    }
    @RequestMapping(value = "/acknowledgement", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> acknowledgementReceived(@RequestBody Map<String,String> request) {
        String consumerName=request.get("consumer_name");
        Long ack=Long.parseLong(request.get("message_id"));

        logger.info("Acknowledgement call received for consumer name "+consumerName+" with ack sequence number "+ ack);
        try{
            consumerService.acknowledgementReceived(consumerName,ack);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Acknowledgement received");
        } catch (ConsumerNotFoundException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage()+" please try to re evaluate the request parameters");
        }
    }
    @RequestMapping(value = "/heartbeat", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> heartbeatReceived(@RequestBody Map<String,String> request) {
        String consumerName=request.get("consumer_name");
        logger.info("Heartbeat call received for consumer name "+consumerName);
        try{
            consumerService.heartBeatRecieved(consumerName);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Heartbeat received");
        } catch (ConsumerNotFoundException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage()+" please try to re evaluate the request parameters");
        }
    }
}
