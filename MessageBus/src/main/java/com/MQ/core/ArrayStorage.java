package com.MQ.core;

import com.MQ.Exception.PartitionIsEmptyException;

import java.util.ArrayList;
import java.util.List;

public class ArrayStorage<K> implements StorageStrategy<K> {

    private List<K> storage;


    public ArrayStorage() {
        this.storage=new ArrayList<>();
    }

    @Override
    public K getElement(int offset) throws PartitionIsEmptyException {

        if(offset>=this.storage.size())
            throw new PartitionIsEmptyException("Offset is out of partition size");
        return this.storage.get(offset);

    }

    @Override
    public void storeElement(K message) {
        this.storage.add(message);
    }
}
