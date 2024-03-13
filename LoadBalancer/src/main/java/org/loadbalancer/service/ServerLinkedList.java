package org.loadbalancer.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.loadbalancer.config.ServerConfig;
import org.loadbalancer.exception.ServerNotFoundException;
import org.loadbalancer.models.BackendServer;
import org.loadbalancer.models.ServerLLNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ServerLinkedList {

    private static final Logger logger = LogManager.getLogger(ServerLinkedList.class);
    @Autowired
    private ServerConfig config;
    private volatile ServerLLNode tail,curr;

    private Map<String,Boolean> isActive;

    public ServerLinkedList() {
        this.tail = null;
        this.curr=null;
    }
    private Lock lock = new ReentrantLock();

    @PostConstruct
    public void loadServers()
    {
        logger.info("Initializing server list");
        isActive=new HashMap<>();
        for(int i=0;i<config.getId().size();i++){

            addServer(new BackendServer(config.getId().get(i),config.getAddress().get(i)));
            isActive.put(config.getId().get(i),true);
        }
        logger.info("Initializing complete");
    }
    public void addServer(BackendServer server)
    {
       try{
           lock.lock();
           logger.info("Add server called for server id "+ server.getServerId());
           ServerLLNode node = new ServerLLNode(server);

           if(tail!=null)
           {
               node.setPrev(tail);
               node.setNext(tail.getNext());
               tail.getNext().setPrev(node);
               tail.setNext(node);
           }
           else {
               node.setPrev(node);
               node.setNext(node);
           }
           tail=node;

           if(curr==null)
               curr=node;

           isActive.put(server.getServerId(),true);
       }finally {
           logger.info("Server added for server id "+ server.getServerId());
           lock.unlock();
       }
    }

    public void removeServer(String serverId) throws ServerNotFoundException
    {
        if(!this.isActive.getOrDefault(serverId,false))
            return ;
       try{
           lock.lock();
           ServerLLNode currNode = curr;

           logger.info("remove server called "+serverId);

           if(currNode==null)
               throw new ServerNotFoundException("Linked list is empty");

           while(currNode.getNext()!=curr && !currNode.getServer().getServerId().equalsIgnoreCase(serverId))
           {
               currNode=currNode.getNext();
           }

           if(currNode.getServer().getServerId().equalsIgnoreCase(serverId))
           {
               logger.info("found the server "+currNode.getServer().getServerId());
               if(currNode.getNext()==currNode)
               {
                   tail=null;
                   curr=null;
                   logger.info("Linked list is now empty");
               }
               else {

                   currNode.getPrev().setNext(currNode.getNext());
                   currNode.getNext().setPrev(currNode.getPrev());

                   if(this.curr==currNode) {
                       this.curr = currNode.getNext();
                       logger.info("curr node shifted to "+ this.curr.getServer().getServerId());
                   }

                   if(this.tail==currNode){
                       this.tail=currNode.getPrev();
                      logger.info("tail node shifted to "+ this.tail.getServer().getServerId());
                   }

                   currNode.setNext(null);
                   currNode.setPrev(null);

               }

               isActive.put(serverId,false);
           }
           else {
               throw new ServerNotFoundException("Server node not present in list");
           }
       }finally {
           logger.info("Removed server "+serverId);
           lock.unlock();
       }
    }

    public BackendServer getNextServer() throws ServerNotFoundException {

        try{
            lock.lock();
            logger.info("Get next server called ");
            if(this.curr==null)
                throw new ServerNotFoundException(" Server list is empty");

            BackendServer ret = this.curr.getServer();
            this.curr=this.curr.getNext();
            logger.info("Next server is "+ this.curr.getServer().getServerId());
            return ret;
        }finally {
            lock.unlock();
        }
    }

    public void addBackServer(int i) {

        if(this.isActive.getOrDefault(config.getId().get(i),false))
            return ;
        try{
            lock.lock();
            logger.info("Add back server called ");

            if(!isActive.getOrDefault(config.getId().get(i),false))
            {
                addServer(new BackendServer(config.getId().get(i),config.getAddress().get(i)));
            }
        }
        finally {
            lock.unlock();
        }
    }
}
