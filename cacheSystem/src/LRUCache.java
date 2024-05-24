public class LRUCache<K,V> implements Cache<K,V>{

    private CacheStorage<K,V> storage;

    public LRUCache(CacheStorage<K,V> storage) {
        this.storage = storage;
    }

    @Override
    public V getKey(K key) throws CacheNotFoundException, CacheIsEmpty {
            return storage.getKey(key);
    }

    @Override
    public void addToCache(K key, V value) throws CacheIsEmpty {

        try{
            storage.addToCache(key,value);
        } catch (CacheOutOfStorage e) {
            storage.evictCache();
            try {
                storage.addToCache(key,value);
            } catch (CacheOutOfStorage ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
