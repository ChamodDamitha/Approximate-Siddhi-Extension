/*
* Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.extension.siddhi.execution.approximate.percentile.tdigest;

import com.sun.org.apache.xml.internal.utils.IntStack;

import java.util.Arrays;


/**
 * An AVL-tree structure of integers used to effectively insert and search data
 * the time complexity for searching is O(lg(n)), n is the number of nodes
 */
public abstract class AVLTree {

    protected static final int NIL = 0;


    private final NodeAllocator nodeAllocator;
    private int rootNode;
    private int[] parentNodes;
    private int[] leftNodes;
    private int[] rightNodes;
    private byte[] depths;

    AVLTree(int initialCapacity) {
        nodeAllocator = new NodeAllocator();
        rootNode = NIL;
        parentNodes = new int[initialCapacity];
        leftNodes = new int[initialCapacity];
        rightNodes = new int[initialCapacity];
        depths = new byte[initialCapacity];
    }

    /**
     * default tree size = 16
     */
    AVLTree() {
        this(16);
    }

    /**
     * Return the current root of the tree.
     */
    public int root() {
        return rootNode;
    }

    /**
     * Return the current capacity, which is the number of nodes that this tree
     * can hold.
     */
    public int capacity() {
        return parentNodes.length;
    }


    /** Grow the tree size by 1/8 */
    static int oversize(int size) {
        return size + (size >>> 3);
    }

    /**
     * Resize internal storage in order to be able to store data for nodes up to
     * newCapacity
     */
    protected void resize(int newCapacity) {
        parentNodes = Arrays.copyOf(parentNodes, newCapacity);
        leftNodes = Arrays.copyOf(leftNodes, newCapacity);
        rightNodes = Arrays.copyOf(rightNodes, newCapacity);
        depths = Arrays.copyOf(depths, newCapacity);
    }

    /**
     * Return the size of this tree.
     */
    public int size() {
        return nodeAllocator.size();
    }

    /**
     * Return the setParentNode Node of the provided node.
     */
    public int parentNode(int node) {
        return parentNodes[node];
    }

    /**
     * Return the setLeftNode child node of the provided node.
     */
    public int leftNode(int node) {
        return leftNodes[node];
    }

    /**
     * Return the setRightNode child node of the provided node.
     */
    public int rightNode(int node) {
        return rightNodes[node];
    }

    /**
     * Return the setDepth of nodes that are stored below node including itself.
     */
    public int depth(int node) {
        return depths[node];
    }

    /**
     * Return the least node under node.
     */
    public int leastNode(int node) {
        while (true) {
            final int left = leftNode(node);
            if (left == NIL) {
                break;
            }
            node = left;
        }
        return node;
    }

    /**
     * Return the largest node under node
     */
    public int last(int node) {
        while (true) {
            final int right = rightNode(node);
            if (right == NIL) {
                break;
            }
            node = right;
        }
        return node;
    }

    /**
     * Return the least node that is strictly greater than node
     */
    public final int nextNode(int node) {
        final int right = rightNode(node);
        if (right != NIL) {
            return leastNode(right);
        } else {
            int parent = parentNode(node);
            while (parent != NIL && node == rightNode(parent)) {
                node = parent;
                parent = parentNode(parent);
            }
            return parent;
        }
    }

    /**
     * Return the largest node that is strictly less than node
     */
    public final int previousNode(int node) {
        final int left = leftNode(node);
        if (left != NIL) {
            return last(left);
        } else {
            int parent = parentNode(node);
            while (parent != NIL && node == leftNode(parent)) {
                node = parent;
                parent = parentNode(parent);
            }
            return parent;
        }
    }

    /**
     * Compare data against data which is stored in node
     */
    protected abstract int compare(int node);

    /**
     * Compare data into node
     */
    protected abstract void copyNewData(int node);


    /**
     * Add current data to the tree and return true if a new node was added
     * to the tree or false if not
     */
    public boolean add() {

//        if the tree is empty
        if (rootNode == NIL) {
            rootNode = nodeAllocator.newNode();
            copyNewData(rootNode);
            fixAggregateCounts(rootNode);
            return true;
        } else { //        tree is not empty
            int node = rootNode;
//            assert parentNode(rootNode) == NIL;
            int parent; // to track the final node
            int cmp;
            do {
                cmp = compare(node);
                if (cmp < 0) {
                    parent = node;
                    node = leftNode(node);
                } else if (cmp > 0) {
                    parent = node;
                    node = rightNode(node);
                } else {
                    return false;
                }
            } while (node != NIL);

            node = nodeAllocator.newNode();
            if (node >= capacity()) {
                resize(oversize(node + 1));
            }
            copyNewData(node);

//            link the node to the tree
            setParentNode(node, parent);
            if (cmp < 0) {
                setLeftNode(parent, node);
            } else {
//                assert cmp > 0;
                setRightNode(parent, node);
            }

            rebalance(node);

            return true;
        }
    }

    /**
     * Update node with the current data.
     */
    public void updateNode(int node) {
        //        find the closest nodes from both sides
        int prev = previousNode(node);
        int next = nextNode(node);

//      if node value is in between prev and next values
        if ((prev == NIL || compare(prev) > 0) && (next == NIL || compare(next) < 0)) {
            copyNewData(node);
            for (int n = node; n != NIL; n = parentNode(n)) {
                fixAggregateCounts(n);
            }
        } else {
            removeNode(node);
            add();
        }
    }

    /**
     * Remove the specified node from the tree.
     */
    public void removeNode(int node) {
        if (node == NIL) {
            throw new IllegalArgumentException();
        }

//        have both children
        if (leftNode(node) != NIL && rightNode(node) != NIL) {
            int next = nextNode(node);
//            assert next != NIL;
            swapNodes(node, next);
        }

//        have less than 2 children
//        assert leftNode(node) == NIL || rightNode(node) == NIL;

        int parent = parentNode(node);
        int child = leftNode(node);
        if (child == NIL) {
            child = rightNode(node);
        }

//        have no children
        if (child == NIL) {
            if (node == rootNode) {
//                assert size() == 1 : size();
                rootNode = NIL;
            } else {
                if (node == leftNode(parent)) {
                    setLeftNode(parent, NIL);
                } else {
//                    assert node == rightNode(parent);
                    setRightNode(parent, NIL);
                }
            }
        } else { //        have 1 child
            if (node == rootNode) {
//                assert size() == 2;
                rootNode = child;
            } else if (node == leftNode(parent)) {
                setLeftNode(parent, child);
            } else {
//                assert node == rightNode(parent);
                setRightNode(parent, child);
            }
            setParentNode(child, parent);
        }

        releaseNode(node);
        rebalance(parent);
    }

    /**
     * release an existing node
     */
    private void releaseNode(int node) {
        setLeftNode(node, NIL);
        setRightNode(node, NIL);
        setParentNode(node, NIL);
        nodeAllocator.release(node);
    }

    /**
     * swap two nodes
     * @param node1
     * @param node2
     */
    private void swapNodes(int node1, int node2) {
        int parent1 = parentNode(node1);
        int parent2 = parentNode(node2);

//      set children of parents of parent1 and parent2
        if (parent1 != NIL) {
            if (node1 == leftNode(parent1)) {
                setLeftNode(parent1, node2);
            } else {
//                assert node1 == rightNode(parent1);
                setRightNode(parent1, node2);
            }
        } else {
//            assert rootNode == node1;
            rootNode = node2;
        }
        if (parent2 != NIL) {
            if (node2 == leftNode(parent2)) {
                setLeftNode(parent2, node1);
            } else {
//                assert node2 == rightNode(parent2);
                setRightNode(parent2, node1);
            }
        } else {
//            assert rootNode == node2;
            rootNode = node1;
        }


//        set parents of node1 and node2
        setParentNode(node1, parent2);
        setParentNode(node2, parent1);

        int left1 = leftNode(node1);
        int left2 = leftNode(node2);
        int right1 = rightNode(node1);
        int right2 = rightNode(node2);


//        set children of node1 and node2
        setLeftNode(node1, left2);
        setLeftNode(node2, left1);
        setRightNode(node1, right2);
        setRightNode(node2, right1);


//        set parents of chlidren of node1 and node2
        if (left2 != NIL) {
            setParentNode(left2, node1);
        }
        if (left1 != NIL) {
            setParentNode(left1, node2);
        }

        if (right2 != NIL) {
            setParentNode(right2, node1);
        }
        if (right1 != NIL) {
            setParentNode(right1, node2);
        }


//        set depths of node1 and node2
        int depth1 = depth(node1);
        int depth2 = depth(node2);
        setDepth(node1, depth2);
        setDepth(node2, depth1);
    }

    /**
     * calculate the difference of depths of left sub tree and right sub tree
     * @param node
     * @return the difference of depths
     */
    private int balanceFactor(int node) {
        return depth(leftNode(node)) - depth(rightNode(node));
    }


    /**
     * rebalance the tree to minimize the difference of depths between right and left sub trees
     * @param node arround which the rebalancing occurs
     */
    private void rebalance(int node) {
        for (int n = node; n != NIL; ) {
            int p = parentNode(n);

            fixAggregateCounts(n);

            switch (balanceFactor(n)) {
                case -2:
                    int right = rightNode(n);
                    if (balanceFactor(right) == 1) {
                        rotateRight(right);
                    }
                    rotateLeft(n);
                    break;
                case 2:
                    int left = leftNode(n);
                    if (balanceFactor(left) == -1) {
                        rotateLeft(left);
                    }
                    rotateRight(n);
                    break;
                case -1:
                case 0:
                case 1:
                    break; // ok
                default:
                    throw new AssertionError();
            }

            n = p;
        }
    }

    /**
     * set the aggregate count of the node based on the maximum aggregate count of left and right child
     * @param node is the node
     */
    protected void fixAggregateCounts(int node) {
        setDepth(node, 1 + Math.max(depth(leftNode(node)), depth(rightNode(node))));
    }

    /**
     * Rotate the tree arround node to clockwise direction
     * @param node is the node
     */
    private void rotateLeft(int node) {
        int r = rightNode(node);
        int lr = leftNode(r);
        setRightNode(node, lr);
        if (lr != NIL) {
            setParentNode(lr, node);
        }
        int p = parentNode(node);
        setParentNode(r, p);
        if (p == NIL) {
            rootNode = r;
        } else if (leftNode(p) == node) {
            setLeftNode(p, r);
        } else {
//            assert rightNode(p) == node;
            setRightNode(p, r);
        }
        setLeftNode(r, node);
        setParentNode(node, r);
        fixAggregateCounts(node);
        fixAggregateCounts(parentNode(node));
    }

    /**
     * Rotate the tree arround node to anti-clockwise direction
     * @param node is the node
     */
    private void rotateRight(int node) {
        int l = leftNode(node);
        int rl = rightNode(l);
        setLeftNode(node, rl);
        if (rl != NIL) {
            setParentNode(rl, node);
        }
        int p = parentNode(node);
        setParentNode(l, p);
        if (p == NIL) {
            rootNode = l;
        } else if (rightNode(p) == node) {
            setRightNode(p, l);
        } else {
//            assert leftNode(p) == node;
            setLeftNode(p, l);
        }
        setRightNode(l, node);
        setParentNode(node, l);
        fixAggregateCounts(node);
        fixAggregateCounts(parentNode(node));
    }

    private void setParentNode(int node, int parent) {
//        assert node != NIL;
        this.parentNodes[node] = parent;
    }

    private void setLeftNode(int node, int left) {
//        assert node != NIL;
        this.leftNodes[node] = left;
    }

    private void setRightNode(int node, int right) {
//        assert node != NIL;
        this.rightNodes[node] = right;
    }

    private void setDepth(int node, int depth) {
//        assert node != NIL;
//        assert depth >= 0 && depth <= Byte.MAX_VALUE;
        this.depths[node] = (byte) depth;
    }





    //    allocate new nodes and release nodes
    private static class NodeAllocator {

        private int nextNode;
        private IntStack releasedNodes;

        NodeAllocator() {
            nextNode = NIL + 1;
            releasedNodes = new IntStack();
        }

        int newNode() {

//          release an existing one
            if (releasedNodes.size() > 0) {
                return releasedNodes.pop();
            } else {
                return nextNode++;
            }
        }


        //      used node is made available for reuse
        void release(int node) {
//            assert node < nextNode;
            releasedNodes.push(node);
        }

        //      number of nodes in the tree
        int size() {
            return nextNode - releasedNodes.size() - 1;
        }

    }

}
