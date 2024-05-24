public class LeastRecentlyUsedStrategy<K,V>  implements EvictionStrategy<K,V>{
    public LeastRecentlyUsedStrategy() {

    }

    @Override
    public void evictCache(LinkedListStorage<K,V> storage) throws CacheIsEmpty {
        DoublyLinkedListNode<K,V> tail = storage.getTail();
        if(tail==null)
            throw new CacheIsEmpty();
        else{

            System.out.println("evicting "+ tail.getKey());
            storage.getNodeList().remove(tail.getKey());
            DoublyLinkedListNode<K,V> nextNode = tail.getNext();
            tail.setNext(null);
            tail=nextNode;
            tail.setPrev(null);

            storage.setTail(tail);
        }
    }

    @Override
    public void evictCache(LinkedListStorage<K,V> storage, DoublyLinkedListNode<K,V> node) {

        DoublyLinkedListNode<K,V> head= storage.getHead();
        DoublyLinkedListNode<K,V> tail = storage.getTail();

        if(node!=head) {
            DoublyLinkedListNode<K, V> prevNode = node.getPrev();
            if (prevNode != null)
                prevNode.setNext(node.getNext());

            DoublyLinkedListNode<K, V> nextNode = node.getNext();
            if (nextNode != null)
                nextNode.setPrev(prevNode);

            node.setPrev(head);

            if (head != null)
                head.setNext(node);
            node.setNext(null);

            tail = nextNode;

            storage.setHead(head);
            storage.setTail(tail);

        }
    }

}
