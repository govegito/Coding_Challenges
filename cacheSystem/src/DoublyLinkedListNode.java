public class DoublyLinkedListNode<K,V>{

    private DoublyLinkedListNode<K,V> next, prev;
    private K key;
    private V value;

    public DoublyLinkedListNode(K key, V value)
    {
        this.key=key;
        this.value=value;
        next=null;
        prev=null;
    }

    public DoublyLinkedListNode<K,V> getNext() {
        return next;
    }

    public void setNext(DoublyLinkedListNode<K,V> next) {
        this.next = next;
    }

    public DoublyLinkedListNode<K,V> getPrev() {
        return prev;
    }

    public void setPrev(DoublyLinkedListNode<K,V> prev) {
        this.prev = prev;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
