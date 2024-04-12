package com.MQ.core;

import com.MQ.Exception.PartitionIsEmptyException;

public interface StorageStrategy<K> {

    public K getElement(int offset) throws PartitionIsEmptyException;
    public void storeElement(K message);
}
