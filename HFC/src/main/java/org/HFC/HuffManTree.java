package org.HFC;

import java.util.HashMap;
import java.util.Map;

public class HuffManTree<K,V> implements Tree<K,V> {

    private TreeNode<K,V> root;
    private Map<K,String> encodings;
    public HuffManTree(TreeNode<K, V> root) {
        this.root = root;
    }

    public Map<K,String> getEncodings(){

        this.encodings = new HashMap<>();
        traverseTree(this.root,"");
        return this.encodings;
    }

    public void traverseTree(TreeNode<K,V> curr, String path)
    {
        if(curr==null)
            return;

        if(curr.getIsleaf())
        {
            this.encodings.put(curr.getKey(),path);
        }

        traverseTree(curr.getLeftChild(),path+"0");
        traverseTree(curr.getRightChild(),path+"1");


    }
}
