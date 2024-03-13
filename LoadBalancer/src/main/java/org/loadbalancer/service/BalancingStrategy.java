package org.loadbalancer.service;

import org.loadbalancer.models.BackendServer;

import java.util.List;

public interface BalancingStrategy {


    public BackendServer getNext();

    public void addServer(BackendServer server);

    public void removeServer(String serverId);


}
