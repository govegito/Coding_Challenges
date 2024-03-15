package org.loadbalancer.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.loadbalancer.config.ServerConfig;
import org.loadbalancer.exception.ServerNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;


public class HealthCheckRunnable implements Runnable{

    private static final Logger logger = LogManager.getLogger(HealthCheckRunnable.class);
    private volatile RestTemplate template;
    private volatile BalancingStrategy strategy;

    private final String serverId;
    private final String address;

    public HealthCheckRunnable(RestTemplate template, BalancingStrategy strategy, String serverId, String address) {
        this.template = template;
        this.strategy = strategy;
        this.serverId=serverId;
        this.address=address;
    }

    @Override
    public void run() {
        String responge=null;
        try{
            responge= template.getForObject(address,String.class);
        }catch(Exception e)
        {
            logger.error(e.getMessage());
        }

        if(responge!=null)
        {
            logger.info("Server is up "+serverId);
            strategy.addBackServer(serverId);
        }
        else {
            logger.info("Calling server out "+ serverId);
            try {
                strategy.removeServer(serverId);
            } catch (Exception e) {
                logger.error(e.getMessage());
                Thread.currentThread().interrupt();

            }
        }
    }
}
