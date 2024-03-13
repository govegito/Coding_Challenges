package org.loadbalancer.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.loadbalancer.config.ServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class HealthCheck {

    @Autowired
    private ServerLinkedList list;

    @Autowired
    private ServerConfig config;

    @Autowired
    private RestTemplate restTemplate;
    private Thread backgroundThread;
    private volatile boolean running = false;

    @PostConstruct
    private void startHealthCheck()
    {
        if(backgroundThread==null || !backgroundThread.isAlive())
        {
            running=true;
            backgroundThread = new Thread(()->{
                while(running)
                {
                    try{
                        for(int i=0;i<config.getAddress().size();i++)
                        {
                            HealthCheckRunnable runnable = new HealthCheckRunnable(restTemplate,list,config,i);
                            new Thread(runnable).start();
                        }
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        running = false;
                    }
                }
            });
            backgroundThread.start();
        }
    }

    @PreDestroy
    private void stopBackgroundTask() {
        running = false;
        if (backgroundThread != null) {
            backgroundThread.interrupt();
        }
    }

}
