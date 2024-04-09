package com.MQ.Controllers;

import com.MQ.core.Consumer.ConsumerGroupService;
import com.MQ.core.Topic;
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

import java.util.Map;

@RestController
@RequestMapping("/consumer-group") // Prefix path
public class ConsumerGroupController {
    private static final Logger logger = LogManager.getLogger(ConsumerGroupController.class);

    @Autowired
    private ConsumerGroupService consumerGroupService;

    @RequestMapping(value = "/add", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> addConsumerGroup(@RequestBody Map<String,String> request)
    {
        String groupName=request.get("consumer_group_name");
        logger.info("Add consumer group call received for id "+groupName );

        try{
            consumerGroupService.addConsumerGroup(groupName);
            return ResponseEntity.status(HttpStatus.OK).body("Consumer group added");
        }catch (Exception e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        }
    }

    @RequestMapping(value = "/remove", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> removeConsumerGroup(@RequestBody Map<String,String> request)
    {
        String groupName=request.get("consumer_group_name");
        logger.info("remove consumer group call received for id "+groupName );

        try{
            consumerGroupService.removeConsumerGroup(groupName);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Consumer group added");
        }catch (Exception e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        }
    }
}
