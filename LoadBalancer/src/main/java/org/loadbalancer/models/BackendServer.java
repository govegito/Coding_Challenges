package org.loadbalancer.models;


public class BackendServer {

    private String serverId;
    private String address;

    public BackendServer(String serverId, String address) {
        this.serverId = serverId;
        this.address = address;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
