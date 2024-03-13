package org.loadbalancer.models;

public class ServerLLNode {

    private final BackendServer server;
    private ServerLLNode next, prev;

    public ServerLLNode(BackendServer server) {
        this.server = server;
        this.next = null;
        this.prev=null;
    }

    public BackendServer getServer() {
        return server;
    }
    public ServerLLNode getNext() {
        return next;
    }
    public void setNext(ServerLLNode next) {
        this.next = next;
    }

    public ServerLLNode getPrev() {
        return prev;
    }

    public void setPrev(ServerLLNode prev) {
        this.prev = prev;
    }
}
