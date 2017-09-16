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
package org.wso2.extension.siddhi.execution.approximate.similarity;

import java.util.Random;

/**
 * Probabilistic data structure to calculate the similarity of two data sets
 *
 * @param <E> is the type of data to be checked for similarity
 */
public class MinHash<E> {
    private int noOfHashFunctions;
    private int noOfSimilarities;

    private int[] firstMinHashValues;
    private int[] secondMinHashValues;

    private double accuracy;

    /**
     * Create a Minhash based on a specified accuracy
     * @param accuracy
     */
    public MinHash(double accuracy) {
        int noOfHashFunctions = (int) Math.ceil(1 / (accuracy * accuracy));
        initMinHash(noOfHashFunctions);
        setAccuracy(accuracy);
    }

    /**
     * Set the accuracy of Minhash
     *
     * @param accuracy
     */
    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    /**
     * Return the accuracy of Minhash
     *
     * @return a double value of the accuracy
     */
    public double getAccuracy() {
        return accuracy;
    }

    /**
     * Initialize the MinHash by specifying the number of signatures(correspond to hash functions)
     *
     * @param noOfHashFunctions is the number of hash functions
     */
    private void initMinHash(int noOfHashFunctions) {
        this.noOfSimilarities = 0;
        this.noOfHashFunctions = noOfHashFunctions;
        firstMinHashValues = new int[noOfHashFunctions];
        secondMinHashValues = new int[noOfHashFunctions];

//      set max int value in the hash value set
        for (int i = 0; i < noOfHashFunctions; i++) {
            firstMinHashValues[i] = Integer.MAX_VALUE;
            secondMinHashValues[i] = Integer.MAX_VALUE;
        }
    }

    /**
     * Adding the new property to the MinHash by calculating their signatures
     *
     * @param firstSetValue
     * @param secondSetValue
     * @return
     */
    public boolean addProperty(E firstSetValue, E secondSetValue) {
        int[][] hashValues = getHashValues(firstSetValue, secondSetValue, noOfHashFunctions);
        int[] firstHashValues = hashValues[0];
        int[] secondHashValues = hashValues[1];

        int tempFirstMinHash;
        int tempFirstHash;
        int tempSecondMinHash;
        int tempSecondHash;

        noOfSimilarities = 0;

        for (int i = 0; i < noOfHashFunctions; i++) {
            tempFirstMinHash = firstMinHashValues[i];
            tempFirstHash = firstHashValues[i];
            tempSecondMinHash = secondMinHashValues[i];
            tempSecondHash = secondHashValues[i];

//          Update the minimum hash values
            if (tempFirstHash < tempFirstMinHash) {
                firstMinHashValues[i] = tempFirstHash;
            }
            if (tempSecondHash < tempSecondMinHash) {
                secondMinHashValues[i] = tempSecondHash;
            }
//          Check for similar hash values
            if (firstMinHashValues[i] == secondMinHashValues[i]) {
                noOfSimilarities++;
            }
        }
        return true;
    }

    /**
     * Get the similarity of the two sets
     *
     * @return the similarity as a percentage
     */
    public double getSimilarity() {
        return ((double) noOfSimilarities / noOfHashFunctions) * 100;
    }

    /**
     * Calculate hash values for items from two sets
     *
     * @param item1      is the item from first set
     * @param item2      is the item from second set
     * @param noOfHashes is the number of hash values needed
     * @return a 2D integer array containing hash values for each item
     */
    private int[][] getHashValues(E item1, E item2, int noOfHashes) {
        final int primeConstant = 1540483477;

        int[] firstHashCodes = new int[noOfHashes];
        int[] secondHashCodes = new int[noOfHashes];
        int hstart1 = item1.hashCode();
        int hstart2 = item2.hashCode();
        int tempRandomInt;
        Random rnd = new Random();

        for (int i = 0; i < noOfHashes; i++) {
            tempRandomInt = Math.abs(rnd.nextInt());
            firstHashCodes[i] = ((hstart1 * (i * 2 + 1)) + tempRandomInt) % primeConstant;
            secondHashCodes[i] = ((hstart2 * (i * 2 + 1)) + tempRandomInt) % primeConstant;
        }
        return new int[][]{firstHashCodes, secondHashCodes};
    }


}
