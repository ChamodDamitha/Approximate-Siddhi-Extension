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

import java.util.Arrays;

/**
 * A tree of t-digest centroids.
 */
final class AVLGroupTree {

    /**
     * for insertion purposes
     */
    private double centroid;
    private int count;

    /**
     * for saving all the data
     */
    private double[] centroids;
    private int[] counts;
    private int[] aggregatedCounts;


    private final AVLTree tree;

    AVLGroupTree() {
        tree = new AVLTree() {
            /**
             * increasing the capacity of the tree and the arrays
             * @param newCapacity
             */
            @Override
            protected void resize(int newCapacity) {
                super.resize(newCapacity);
                centroids = Arrays.copyOf(centroids, newCapacity);
                counts = Arrays.copyOf(counts, newCapacity);
                aggregatedCounts = Arrays.copyOf(aggregatedCounts, newCapacity);
            }

            /**
             * Copy the newly arrive data to the given node
             * @param node
             */
            @Override
            protected void copyNewData(int node) {
                centroids[node] = centroid;
                counts[node] = count;
            }

            /**
             * the current centroid is compared with the centroid in the given node
             * @param node
             * @return -1 if current centroid is lesser, 0 or 1 if current centroid is larger
             */
            @Override
            protected int compare(int node) {
                if (centroid < centroids[node]) {
                    return -1;
                } else {
                    return 1;
                }
            }

            /**
             * set the total number of nodes up to the given node from bottom
             * @param node is the node
             */
            @Override
            protected void fixAggregateCounts(int node) {
                super.fixAggregateCounts(node);
                aggregatedCounts[node] = counts[node] + aggregatedCounts[leftNode(node)] +
                        aggregatedCounts[rightNode(node)];
            }

        };

        centroids = new double[tree.capacity()];
        counts = new int[tree.capacity()];
        aggregatedCounts = new int[tree.capacity()];

    }

    /**
     * Return the number of centroids in the tree.
     */
    public int size() {
        return tree.size();
    }

    /**
     * Return the previous node.
     */
    public int previousNode(int node) {
        return tree.previousNode(node);
    }

    /**
     * Return the nextNode node.
     */
    public int nextNode(int node) {
        return tree.nextNode(node);
    }

    /**
     * Return the mean for the provided node.
     */
    public double mean(int node) {
        return centroids[node];
    }

    /**
     * Return the count for the provided node.
     */
    public int count(int node) {
        return counts[node];
    }


    /**
     * Add the provided centroid to the tree.
     */
    public void add(double centroid, int count) {
        this.centroid = centroid;
        this.count = count;
        tree.addValue();
    }

    /**
     * Update values associated with a node
     */
    public void update(int node, double centroid, int count) {
        this.centroid = centroid;
        this.count = count;
        tree.updateNode(node);
    }

    /**
     * Return the largest node whose centroid is less than the given centroid
     */
    public int floorNode(double centroid) {
        int floor = AVLTree.NIL;
        for (int node = tree.root(); node != AVLTree.NIL; ) {
            int cmp = Double.compare(centroid, mean(node));
            if (cmp <= 0) {
                node = tree.leftNode(node);
            } else {
                floor = node;
                node = tree.rightNode(node);
            }
        }
        return floor;
    }

    /**
     * Return the largestNode node so that the sum of counts of nodes that are before
     * it is less than or equal to sum
     */
    public int floorSumNode(long sum) {
        int floor = AVLTree.NIL;
        for (int node = tree.root(); node != AVLTree.NIL; ) {
            int left = tree.leftNode(node);
            long leftCount = aggregatedCounts[left];
            if (leftCount <= sum) {
                floor = node;
                sum -= leftCount + count(node);
                node = tree.rightNode(node);
            } else {
                node = tree.leftNode(node);
            }
        }
        return floor;
    }

    /**
     * Return the least node in the tree.
     */
    public int leastNode() {
        return tree.leastNode(tree.root());
    }

    /**
     * Compute the number of elements and sum of counts for every entry that
     * is strictly before node
     */
    public long headSum(int node) {
        int left = tree.leftNode(node);
        long sum = aggregatedCounts[left];
        int n = node;
        int p = tree.parentNode(node);

        while (p != AVLTree.NIL) {
            if (n == tree.rightNode(p)) {
                sum += counts[p] + aggregatedCounts[tree.leftNode(p)];
            }
            n = p;
            p = tree.parentNode(n);
        }

        return sum;
    }


}
