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
    private volatile ServerLinkedList list;
    private volatile ServerConfig config;
    private volatile int t;

    public HealthCheckRunnable(RestTemplate template, ServerLinkedList list, ServerConfig config, int t) {
        this.template = template;
        this.list = list;
        this.config = config;
        this.t = t;
    }

    @Override
    public void run() {
        String responge=null;
        try{
            responge= template.getForObject(config.getAddress().get(t),String.class);
        }catch(Exception e)
        {
            logger.error(e.getMessage());
        }

        if(responge!=null)
        {
            logger.info("Server is up "+config.getId().get(t));
            list.addBackServer(t);
        }
        else {
            logger.info("Calling server out "+ config.getId().get(t));
            try {
                list.removeServer(config.getId().get(t));
            } catch (ServerNotFoundException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
