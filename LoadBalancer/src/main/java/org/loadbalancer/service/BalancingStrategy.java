package org.loadbalancer.service;

import org.loadbalancer.models.BackendServer;

public interface BalancingStrategy {


    public BackendServer getServer();

    public void addServer(BackendServer server);

    public void removeServer(String serverId);


}
