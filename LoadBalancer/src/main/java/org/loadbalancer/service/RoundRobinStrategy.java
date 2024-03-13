package org.loadbalancer.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.loadbalancer.exception.ServerNotFoundException;
import org.loadbalancer.models.BackendServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;

@Service
public class RoundRobinStrategy implements BalancingStrategy{

    @Autowired
    private ServerLinkedList serverList;
    @Override
    public BackendServer getNext() {
        try {
            return this.serverList.getNextServer();
        } catch (ServerNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addServer(BackendServer server) {
        this.serverList.addServer(server);
    }

    @Override
    public void removeServer(String serverId) {

        try {
            this.serverList.removeServer(serverId);
        } catch (ServerNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
