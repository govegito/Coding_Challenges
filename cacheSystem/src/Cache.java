public interface Cache<K,V> {

     V getKey(K key) throws CacheNotFoundException, CacheIsEmpty;

     void addToCache(K key, V value) throws CacheIsEmpty;
}
