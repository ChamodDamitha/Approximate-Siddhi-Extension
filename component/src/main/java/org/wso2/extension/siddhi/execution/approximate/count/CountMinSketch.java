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
package org.wso2.extension.siddhi.execution.approximate.count;

import org.wso2.extension.siddhi.execution.approximate.cardinality.MurmurHash;

import java.io.Serializable;
import java.util.Random;

/**
 * A probabilistic data structure to keep count of different items.
 *
 * @param <E> is the type of data to be counted
 */
public class CountMinSketch<E> implements Serializable{

    private int depth;
    private int width;

    private long totalNoOfItems;

    //  2D array to store the counts
    private long[][] countArray;

    //  hash coefficients
    private int[] hashCoefficients_A;
    private int[] hashCoefficients_B;

    //  Error factors of approximation
    private double relativeError;
    private double confidence;


    /**
     * instantiate the count min sketch based on a given relativeError and confidence
     * approximate_answer - (relativeError * numberOfInsertions) <= actual_answer
     * <= approximate_answer + (relativeError * numberOfInsertions)
     *
     * @param relativeError is a positive number less than 1 (e.g. 0.01)
     * @param confidence    is a positive number less than 1 (e.g. 0.01)
     *                      which is the probability of answers being within the relative error
     */
    public CountMinSketch(double relativeError, double confidence) {
        if (!(relativeError < 1 && relativeError > 0) || !(confidence < 1 && confidence > 0)) {
            throw new IllegalArgumentException("confidence and relativeError must be values in the range (0,1)");
        }

        this.totalNoOfItems = 0;

        this.relativeError = relativeError;
        this.confidence = confidence;

//      depth = ln(1 / (1 - confidence))
        this.depth = (int) Math.ceil(Math.log(1 / (1 - confidence)));
//      width = e / relativeError
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
    }

    /**
     * Compute the cell position in a row of the count array for a given hash value
     *
     * @param hash is the integer hash value generated from some hash function
     * @return an integer value in the range [0,width)
     */
    private int getArrayIndex(int hash) {
        return Math.abs(hash % width);
    }


    /**
     * Compute a set of different integer hash values for a given item
     *
     * @param item is the object for which the hash values are calculated
     * @return an int array(of size {@code depth}) of hash values
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
     * calculate hash values relevant for each row in the count array
     * compute indices in the range of [0, width) from those hash values
     * increment each value in the cell of relevant row and index (e.g. countArray[row][index]++)
     *
     * @param item
     * @return a long array which contains the approximate count, lower bound and the upper bound
     * of the confidence interval consecutively
     */
    public long[] insert(E item) {
        totalNoOfItems++;

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

        return getConfidenceInterval(currentMin + 1);
    }

    /**
     * Removes the count of an item from the count min sketch
     * calculate hash values relevant for each row in the count array
     * compute indices in the range of [0, width) from those hash values
     * decrement each value in the cell of relevant row and index (e.g. countArray[row][index]--)
     *
     * @param item
     * @return a long array which contains the approximate count, lower bound and the upper bound
     * of the confidence interval consecutively
     */
    public long[] remove(E item) {
        totalNoOfItems--;

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
        return getConfidenceInterval(currentMin - 1);
    }

    /**
     * Compute the approximate count for a given item.
     * Check the relevant cell values for the given item by hashing it to cell indices.
     * Then take the minimum out of those values.
     * @param item to be counted
     * @return a long array which contains the approximate count, lower bound and the upper bound
     * of the confidence interval consecutively
     */
    public long[] approximateCount(E item) {
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
            return new long[]{0, 0, 0};
        }
//      if item is found
        return getConfidenceInterval(minCount);
    }


    /**
     * Calculate the confidence interval of the approximate count
     * approximateCount - (totalNoOfItems * relativeError) <= exactCount
     * <= approximateCount + (totalNoOfItems * relativeError)
     * @param count
     * @return a long array which contains the count, the lower bound and
     * the upper bound of the confidence interval consecutively
     */
    private long[] getConfidenceInterval(long count) {
        if (count - (long) (totalNoOfItems * relativeError) > 0) {
            return new long[]{count, count - (long) (totalNoOfItems * relativeError),
                    (long) (count + (totalNoOfItems * relativeError))};
        } else {
            return new long[]{count, 0, (long) (count + (totalNoOfItems * relativeError))};
        }
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
