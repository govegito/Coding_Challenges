public class CacheFactory<K,V> {

    public Cache<K,V> getLRUCache(int capacity){

        EvictionStrategy<K,V> evictionStrategy = new LeastRecentlyUsedStrategy();
        CacheStorage<K,V> storage = new LinkedListStorage<>(capacity,evictionStrategy);

        Cache<K,V> cache = new LRUCache<>(storage);

        return cache;
    }
}
