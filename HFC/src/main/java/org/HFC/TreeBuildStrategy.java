package org.HFC;

import java.util.Map;

public interface TreeBuildStrategy<K,V> {

    public TreeNode<K,V> BuildTree(Map<K,V> list);
}
