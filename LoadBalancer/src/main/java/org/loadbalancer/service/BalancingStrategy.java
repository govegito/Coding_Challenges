package org.loadbalancer.service;

import org.loadbalancer.models.BackendServer;

public interface BalancingStrategy {


    public BackendServer getServer(String key);

    public void addServer(BackendServer server);

    public void addBackServer(String serverId);

    public void removeServer(String serverId);


}
