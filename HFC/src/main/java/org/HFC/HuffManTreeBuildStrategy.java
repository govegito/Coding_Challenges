package org.HFC;

import java.util.Map;
import java.util.PriorityQueue;

public class HuffManTreeBuildStrategy<K,V> implements TreeBuildStrategy<K,V> {

    @Override
    public TreeNode<K,V> BuildTree(Map<K,V> nodeList) throws IllegalArgumentException {

        PriorityQueue<TreeNode<K,V>> minHeap = new PriorityQueue<>();

        for(K key: nodeList.keySet())
        {
            minHeap.add(new TreeNode<>(key,nodeList.get(key)));
        }

        while(minHeap.size()>1)
        {
            TreeNode<K,V> mergedNode = mergeNode(minHeap.poll(),minHeap.poll());
            minHeap.add(mergedNode);
        }

        return minHeap.poll();
    }

    public TreeNode<K,V> mergeNode(TreeNode<K,V> node1, TreeNode<K,V> node2) throws IllegalArgumentException
    {
        TreeNode<K,V> newNode = new TreeNode<>(null,addValues(node1.getValue(),node2.getValue()));
        newNode.setLeftChild(node1);
        newNode.setRightChild(node2);
        newNode.setIsleaf(false);
        return newNode;
    }

    private V addValues(V value1, V value2) throws IllegalArgumentException{
        if (value1 instanceof Integer && value2 instanceof Integer) {
            return (V) Integer.valueOf(((Integer) value1) + ((Integer) value2));
        } else if (value1 instanceof Double && value2 instanceof Double) {
            return (V) Double.valueOf(((Double) value1) + ((Double) value2));
        }
        // Add support for other types as needed
        throw new IllegalArgumentException("Unsupported value types");
    }


}
