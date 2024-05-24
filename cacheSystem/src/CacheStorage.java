import java.util.Optional;

public interface CacheStorage<K,V> {
    public void addToCache(K key, V value) throws CacheOutOfStorage;
    public void evictCache() throws CacheIsEmpty;
    public V getKey(K key) throws CacheNotFoundException, CacheIsEmpty;
}
