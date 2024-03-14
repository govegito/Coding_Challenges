package org.loadbalancer.controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.loadbalancer.config.ServerConfig;
import org.loadbalancer.models.BackendServer;
import org.loadbalancer.service.BalancingStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/test")
public class Controller {

    private static final Logger logger = LogManager.getLogger(Controller.class);

    @Autowired
    private BalancingStrategy strategy;

    @Autowired
    private ServerConfig config;

    @GetMapping("")
    public ResponseEntity<Mono<String>> tryLoadBalancer()
    {
        logger.info("Test load balancer hit");

        try{
            WebClient client = WebClient.create(strategy.getServer().getAddress());
            Mono<String> ret = client.get().retrieve().bodyToMono(String.class).timeout(Duration.ofSeconds(5));
            return  ResponseEntity.status(HttpStatus.ACCEPTED).body(ret);

        }catch (RuntimeException e)
        {
            return  ResponseEntity.status(HttpStatus.NOT_FOUND).body(Mono.just("Server fetch failed"));

        }
    }

    @GetMapping("/remove/{id}")
    public ResponseEntity<String> removeServer(@PathVariable String id)
    {
        try{
            strategy.removeServer(id);
            return  ResponseEntity.status(HttpStatus.ACCEPTED).body("SUCCESS");
        }catch (RuntimeException e)
        {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        }
    }

    @RequestMapping(value = "/add", method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    public ResponseEntity<String> removeServer(@RequestParam Map<String, String> paramMap)
    {
        try {
            String serverId= paramMap.get("serverId");
            String address=paramMap.get("address");
            strategy.addServer(new BackendServer(serverId,address));
            config.addServer(serverId,address);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("SUCCESS");
        }catch (RuntimeException e)
        {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }
}
