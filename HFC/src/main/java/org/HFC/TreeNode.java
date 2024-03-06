package org.HFC;

public class TreeNode<K, V> implements Comparable<TreeNode<K,V>>{

    private K key;
    private V value;

    private Boolean isleaf;
    private TreeNode<K,V> leftChild,rightChild;

    public TreeNode(K key, V value) {
        this.key = key;
        this.value = value;
        this.leftChild=this.rightChild=null;
        this.isleaf=true;
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

    public TreeNode<K, V> getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(TreeNode<K, V> leftChild) {
        this.leftChild = leftChild;
    }

    public TreeNode<K, V> getRightChild() {
        return rightChild;
    }

    public void setRightChild(TreeNode<K, V> rightChild) {
        this.rightChild = rightChild;
    }

    public Boolean getIsleaf() {
        return isleaf;
    }

    public void setIsleaf(Boolean isleaf) {
        this.isleaf = isleaf;
    }

    @Override
    public int compareTo(TreeNode<K, V> other) {
        return Integer.compare((Integer) this.value,(Integer) other.value);
    }
}
