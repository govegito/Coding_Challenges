public interface EvictionStrategy<K,V> {
    public void evictCache(LinkedListStorage<K,V> storage) throws CacheIsEmpty;
    public void evictCache(LinkedListStorage<K,V> storage, DoublyLinkedListNode<K,V> node);
}
