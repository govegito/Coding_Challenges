import java.util.HashMap;
import java.util.Map;

public class LinkedListStorage<K,V> implements CacheStorage<K,V>{

    private final int capacity;
    private Map<K,DoublyLinkedListNode<K,V>> nodeList;
    private final EvictionStrategy<K,V> evictionStrategy;
    private DoublyLinkedListNode<K,V> head;
    private DoublyLinkedListNode<K,V> tail;

    public LinkedListStorage(int capacity, EvictionStrategy evictionStrategy) {
        this.capacity = capacity;
        this.nodeList = new HashMap<>();
        this.head = null;
        this.tail=null;
        this.evictionStrategy=evictionStrategy;
    }

    @Override
    public void addToCache(K key, V value) throws CacheOutOfStorage {

        if(nodeList.size()==capacity)
                throw new CacheOutOfStorage();
        else {

            DoublyLinkedListNode<K, V> node = new DoublyLinkedListNode<>(key, value);
            node.setPrev(head);
            if(head!=null)
             head.setNext(node);
            head=node;

            nodeList.put(key,node);

            if(tail==null)
                tail=node;
        }

    }

    @Override
    public void evictCache() throws CacheIsEmpty{

       evictionStrategy.evictCache(this);
    }

    @Override
    public V getKey(K key) throws CacheNotFoundException, CacheIsEmpty{
        if(nodeList.isEmpty())
            throw new CacheIsEmpty();
        else if(nodeList.containsKey(key))
        {
            DoublyLinkedListNode<K,V> node = nodeList.get(key);

            evictionStrategy.evictCache(this,node);

            return node.getValue();
        }
        else
            throw new CacheNotFoundException();

    }

    public DoublyLinkedListNode<K, V> getHead() {
        return head;
    }

    public void setHead(DoublyLinkedListNode<K, V> head) {
        this.head = head;
    }

    public DoublyLinkedListNode<K, V> getTail() {
        return tail;
    }

    public void setTail(DoublyLinkedListNode<K, V> tail) {
        this.tail = tail;
    }

    public Map<K, DoublyLinkedListNode<K, V>> getNodeList() {
        return nodeList;
    }
}
