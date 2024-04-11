package com.MQ.core;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TopicPartitionBalancingStrategy implements BalancingStrategy{

    private final int partitionCount;

    private int currCounter;
    private final MessageDigest md;


    public TopicPartitionBalancingStrategy(int partitionCount) throws NoSuchAlgorithmException {
        this.partitionCount = partitionCount;
        this.currCounter=0;
        this.md = MessageDigest.getInstance("MD5");;

    }

    @Override
    public Integer getNext(){

        int ret=currCounter%partitionCount;
        currCounter+=1;
        return ret;
    }

    public int getForKey(String key){

        return (int) (generateHash(key)%partitionCount);
    }

    private long generateHash(String key) {
            md.reset();
            md.update(key.getBytes());
            byte[] digest = md.digest();
            long hash = ((long) (digest[3] & 0xFF) << 24) | ((long) (digest[2] & 0xFF) << 16) | ((long) (digest[1] & 0xFF) << 8) | ((long) (digest[0] & 0xFF));
            return hash;

    }
}
