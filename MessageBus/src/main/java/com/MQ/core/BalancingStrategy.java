package com.MQ.core;

public interface BalancingStrategy<T> {

    public T getNext();
}
