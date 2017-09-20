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


import java.util.Random;

/**
 * A tree data structure to store centroids of a set of values
 */
public class AVLTreeDigest extends TDigest {

    private Random gen = new Random();
    private double compression;
    private AVLGroupTree avlGroupTree;
    long count = 0;


    public AVLTreeDigest(double compression) {
        this.compression = compression;
        avlGroupTree = new AVLGroupTree();
    }


    @Override
    public void add(double value, int weight) {

//      set the start node
        checkValue(value);
        int start = avlGroupTree.floorNode(value);
        if (start == AVLTree.NIL) {
            start = avlGroupTree.leastNode();
        }

//        empty tree
        if (start == AVLTree.NIL) {
            avlGroupTree.add(value, weight);
            count = weight;
        } else { //        tree has nodes
            double minDistance = Double.MAX_VALUE;
            int lastNeighbor = AVLTree.NIL;


//          choose the nearest neighbour from either sides
            for (int neighbor = start; neighbor != AVLTree.NIL; neighbor = avlGroupTree.nextNode(neighbor)) {
                double diff = Math.abs(avlGroupTree.mean(neighbor) - value);
                if (diff < minDistance) {
                    start = neighbor;
                    minDistance = diff;
                } else if (diff > minDistance) {
                    // as soon as diff increases, have passed the nearest neighbor and can quit
                    lastNeighbor = neighbor;
                    break;
                }
            }

            int closest = AVLTree.NIL;
            long sum = avlGroupTree.headSum(start);
            double n = 0;
            for (int neighbor = start; neighbor != lastNeighbor; neighbor = avlGroupTree.nextNode(neighbor)) {
                double q;
                if(count == 1) {
                    q = 0.5;
                } else {
                    q = (sum + (avlGroupTree.count(neighbor) - 1) / 2.0) / (count - 1);
                }
                double k = 4 * count * q * (1 - q) / compression;


//              check whether the value can be merged into the neighbour centroid
                if (avlGroupTree.count(neighbor) + weight <= k) {
                    n++;

//                  with the increase of n, the probability of going inside the if condition decrease
                    if (gen.nextDouble() < 1 / n) {
                        closest = neighbor;
                    }
                }
                sum += avlGroupTree.count(neighbor);
            }

            if (closest == AVLTree.NIL) {
                avlGroupTree.add(value, weight);
            } else {
                double centroid = avlGroupTree.mean(closest);
                int count = avlGroupTree.count(closest);

//                merge the value with the centroid
                centroid = weightedAverage(centroid, count, value, weight);
                count += weight;

                avlGroupTree.update(closest, centroid, count);
            }
            count += weight;
        }
    }


    /**
     * calculate the percentile value for a given position
     * @param percentilePosition is the position of percentile in the range [0,1].
     * @return the value of the percentile
     */
    @Override
    public double percentile(double percentilePosition) {
        if (percentilePosition < 0 || percentilePosition > 1) {
            throw new IllegalArgumentException("q should be in [0,1], but found " + percentilePosition);
        }

        AVLGroupTree groupTree = avlGroupTree;

//        empty tree, So no percentile
        if (groupTree.size() == 0) {
            return Double.NaN;
        } else if (groupTree.size() == 1) {
            return groupTree.mean(groupTree.leastNode());
        }//only one centroid available

        System.out.println("count :" + count);//TODO : test

        final double index = percentilePosition * (count - 1);


        System.out.println("index :" + index);//TODO : test

        double previousMean = Double.NaN;
        double previousIndex = 0;
        int next = groupTree.floorSumNode((long) index);
        long total = groupTree.headSum(next);
        int prev = groupTree.previousNode(next);

        if (prev != AVLTree.NIL) {
            previousMean = groupTree.mean(prev);
            previousIndex = total - ((groupTree.count(prev) + 1.0) / 2);
        }


        System.out.println("next :" + next);//TODO : test
        System.out.println("total :" + total);//TODO : test
        System.out.println("prev :" + prev);//TODO : test
        System.out.println("previousMean :" + previousMean);//TODO : test
        System.out.println("previousIndex :" + previousIndex);//TODO : test
        System.out.println("------------------------------------------------------------");//TODO : test

        while (true) {
            double nextIndex = total + ((groupTree.count(next) - 1.0) / 2);

            if (nextIndex >= index) {
                if (Double.isNaN(previousMean)) {
                    if (nextIndex == previousIndex) {
                        return groupTree.mean(next);
                    }
                    int next2 = groupTree.nextNode(next);
                    double nextIndex2 = total + groupTree.count(next) + (groupTree.count(next2) - 1.0) / 2;
                    previousMean = (nextIndex2 * groupTree.mean(next) - nextIndex * groupTree.mean(next2))
                            / (nextIndex2 - nextIndex);
                }
                return percentile(index, previousIndex, nextIndex, previousMean, groupTree.mean(next));
            } else if (groupTree.nextNode(next) == AVLTree.NIL) {
                double nextIndex2 = count - 1;
                double nextMean2 = (groupTree.mean(next) * (nextIndex2 - previousIndex) - previousMean *
                        (nextIndex2 - nextIndex)) / (nextIndex - previousIndex);
                return percentile(index, nextIndex, nextIndex2, groupTree.mean(next), nextMean2);
            }
            total += groupTree.count(next);
            previousMean = groupTree.mean(next);
            previousIndex = nextIndex;
            next = groupTree.nextNode(next);
        }
    }


}
