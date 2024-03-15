package org.loadbalancer.service;

import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.loadbalancer.config.ServerConfig;
import org.loadbalancer.exception.ServerNotFoundException;
import org.loadbalancer.models.BackendServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ConsistentHashingStrategy implements BalancingStrategy{

    private static final Logger logger = LogManager.getLogger(ConsistentHashingStrategy.class);

    @Autowired
    private ServerConfig config;

    private final int numberOfNodes;
    private final Lock lock = new ReentrantLock();


    private final TreeMap<Long,BackendServer> serverRing;
    private final MessageDigest md;

    @Autowired
    public ConsistentHashingStrategy(ServerConfig config) throws NoSuchAlgorithmException {
        this.numberOfNodes = config.getNumberOfVirtualNodes();
        this.serverRing = new TreeMap<>();
        this.md = MessageDigest.getInstance("MD5");;
    }

    @Override
    public BackendServer getServer(String key) {

        try{
            lock.lock();
            logger.info("Get server called for key "+ key);
            if (serverRing.isEmpty()) {
                logger.error("Hash ring is empty");
                throw new RuntimeException("Server list is empty");
            }
            long hash = generateHash(key);
            if (!serverRing.containsKey(hash)) {
                SortedMap<Long, BackendServer> tailMap = serverRing.tailMap(hash);
                hash = tailMap.isEmpty() ? serverRing.firstKey() : tailMap.firstKey();
                logger.info("Returning server "+serverRing.get(hash)+" for the key "+key);
            }
            return serverRing.get(hash);
        }finally {
            lock.unlock();
        }

    }

    @Override
    public void addServer(BackendServer server) {
        try{
            lock.lock();
            logger.info("Adding server to ring "+server.getServerId());
            for (int i = 0; i < numberOfNodes; i++) {
                long hash = generateHash(server.getServerId() + i);
                serverRing.put(hash, server);
            }
        }finally {
            lock.unlock();
        }

    }

    @Override
    public void addBackServer(String serverId) {

        try{
            lock.lock();
            if(!serverRing.containsKey(generateHash(serverId+0)))
            {
                logger.info("Adding server back "+serverId);
                for(int i=0;i<this.config.getId().size();i++)
                {
                    if(this.config.getId().get(i).equalsIgnoreCase(serverId))
                    {
                        addServer(new BackendServer(this.config.getId().get(i),this.config.getAddress().get(i)));
                        break;
                    }
                }
            }
        }finally {
            lock.unlock();
        }


    }

    @Override
    public void removeServer(String serverId) {
        try{
            lock.lock();
            logger.info("Removing server "+serverId);

            for (int i = 0; i < numberOfNodes; i++) {
                long hash = generateHash(serverId + i);
                serverRing.remove(hash);
            }
        }finally {
            lock.unlock();
        }
    }
    private long generateHash(String key) {
        try{
            lock.lock();
            md.reset();
            md.update(key.getBytes());
            byte[] digest = md.digest();
            long hash = ((long) (digest[3] & 0xFF) << 24) | ((long) (digest[2] & 0xFF) << 16) | ((long) (digest[1] & 0xFF) << 8) | ((long) (digest[0] & 0xFF));
            return hash;
        }finally {
            lock.unlock();
        }

    }

    @PostConstruct
    public void addServersinit(){
        for(int i=0;i<this.config.getId().size();i++)
        {
            this.addServer(new BackendServer(this.config.getId().get(i),this.config.getAddress().get(i)));
        }
    }
}
