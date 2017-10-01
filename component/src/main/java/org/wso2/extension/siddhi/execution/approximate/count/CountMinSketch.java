package org.wso2.extension.siddhi.execution.approximate.count;/*
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

import java.util.Random;

/**
 * A probabilistic data structure to keep count of different items
 *
 * @param <E> is the type of data to be counted
 */
public class CountMinSketch<E> {

    private int depth;
    private int width;

    //  2D array to store counts
    private long[][] countArray;

    //  hash coefficients
    private int[] hashCoefficients_A;
    private int[] hashCoefficients_B;

    //  Error factors of approximation
    private double relativeError;
    private double confidence;


    /**
     * instantiate the count min sketch based on a given relativeError and confidence
     * actual_answer <= approximate_answer + (relativeError * numberOfInsertions)
     *
     * @param relativeError is a positive number less than 1 (e.g. 0.01)
     * @param confidence is a positive number less than 1 (e.g. 0.01) which is the probability of reaching the accuracy
     */
    public CountMinSketch(double relativeError, double confidence) {
        if (!(relativeError < 1 && relativeError > 0) || !(confidence < 1 && confidence > 0)) {
            throw new IllegalArgumentException("confidence and relativeError must be values in the range (0,1)");
        }

        this.relativeError = relativeError;
        this.confidence = confidence;

        this.depth = (int) Math.ceil(Math.log(1 / (1 - confidence)));
        this.width = (int) Math.ceil(Math.E / relativeError);

        this.countArray = new long[depth][width];

//      create random hash coefficients
//      using linear hash functions of the form (a*x+b)
//      a,b are chosen independently for each hash function.
        this.hashCoefficients_A = new int[depth];
        this.hashCoefficients_B = new int[depth];
        Random random = new Random(123);
        for (int i = 0; i < depth; i++) {
            hashCoefficients_A[i] = random.nextInt(Integer.MAX_VALUE);
            hashCoefficients_B[i] = random.nextInt(Integer.MAX_VALUE);
        }

        System.out.println("depth : " + depth);//TODO : added for testing
        System.out.println("width : " + width);//TODO : added for testing
    }

    /**
     * Compute a cell position in a row of the count array for a given hash value
     *
     * @param hash is the integer hash value generated from some hash function
     * @return an integer value in the range [0,width)
     */
    private int getArrayIndex(int hash) {
        return Math.abs(hash % width);
    }


    /**
     * Compute a set of different int values for a byte array of data
     *
     * @param item is the object for which the hash values are needed
     * @return an int array of hash values
     */
    private int[] getHashValues(E item) {
        int[] hashValues = new int[depth];
        int hash = MurmurHash.hash(item);
        for (int i = 0; i < depth; i++) {
            hashValues[i] = hashCoefficients_A[i] * hash + hashCoefficients_B[i];
        }
        return hashValues;
    }

    /**
     * Adds the count of an item to the count min sketch
     * calculate hash values for number of row in the count array
     * compute indices in the range of [0, width) from those hash values
     * increment each value in the cell of relevant row and index (e.g. countArray[row][index]++)
     *
     * @param item
     * @return count of the item
     */
    public long insert(E item) {
        int[] hashValues = getHashValues(item);
        int index;
        long currentMin = Long.MAX_VALUE;
        long currentVal;

        for (int i = 0; i < depth; i++) {
            index = getArrayIndex(hashValues[i]);
            currentVal = countArray[i][index];
            countArray[i][index] = currentVal + 1;

            if (currentMin > currentVal) {
                currentMin = currentVal;
            }
        }

        return currentMin + 1;
    }

    /**
     * Removes the count of an item from the count min sketch
     * calculate hash values for number of row in the count array
     * compute indices in the range of [0, width) from those hash values
     * decrement each value in the cell of relevant row and index (e.g. countArray[row][index]--)
     *
     * @param item
     * @return count of the item
     */
    public long remove(E item) {
        int[] hashValues = getHashValues(item);
        int index;
        long currentMin = Long.MAX_VALUE;
        long currentVal;

        for (int i = 0; i < depth; i++) {
            index = getArrayIndex(hashValues[i]);
            currentVal = countArray[i][index];
            countArray[i][index] = currentVal - 1;

            if (currentMin > currentVal) {
                currentMin = currentVal;
            }
        }
        return currentMin - 1;
    }

    /**
     * Compute the approximate count for a given item
     * Check the relevant cell values for the given item by hashing it to cell indices
     * Then take the minimum out of those values
     *
     * @param item
     * @return
     */
    public long approximateCount(E item) {
        int[] hashValues = getHashValues(item);
        int index;

        long minCount = Long.MAX_VALUE;
        long tempCount;

        for (int i = 0; i < depth; i++) {
            index = getArrayIndex(hashValues[i]);
            tempCount = countArray[i][index];
            if (tempCount < minCount) {
                minCount = tempCount;
            }
        }
//      if item not found
        if (minCount == Long.MAX_VALUE) {
            return 0;
        }
//      if item is found
        return minCount;
    }


    /**
     * Return the relativeError of the count min sketch
     *
     * @return
     */
    public double getRelativeError() {
        return relativeError;
    }

    /**
     * Return the confidence of the count min sketch
     *
     * @return
     */
    public double getConfidence() {
        return confidence;
    }
}
