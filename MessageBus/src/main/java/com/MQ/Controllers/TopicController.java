package com.MQ.Controllers;

import com.MQ.Config.ProducerConfig;
import com.MQ.core.ClusterService;
import com.MQ.core.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController("/topic")
public class TopicController {
    private static final Logger logger = LogManager.getLogger(TopicController.class);

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ProducerConfig producerConfig;

    @RequestMapping(value = "/addTopic", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> addTopic(@RequestBody Map<String,String> request)
    {
        String topicName=request.get("topicName");
        logger.info("Add topic call received for topic "+topicName );

        if(!clusterService.ifTopicPresent(topicName))
        {
            try{
                Topic newTopic= new Topic(topicName,producerConfig.getPartitionNumber());
                clusterService.addTopic(newTopic);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(newTopic);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());

            }

        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Topic already exist");
    }
}
