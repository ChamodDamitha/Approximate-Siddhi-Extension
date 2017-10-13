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
package org.wso2.extension.siddhi.execution.approximate.cardinality;

import java.io.Serializable;

/**
 * A probabilistic data structure to calculate the cardinality of a set.
 * The referred research paper - http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf //TODO : name of the paper ,author
 * @param <E> is the type of objects in the set.
 */
public class HyperLogLog<E> implements Serializable{
    private final double standardError = 1.04; //TODO : capital constansts
    private final double pow2of32 = Math.pow(2, 32);

    private int noOfBuckets;
    private int lengthOfBucketId;
    private int noOfZeroBuckets;

    private double estimationFactor;
    private double relativeError;
    private double confidence;
    private double harmonicCountSum;

    private long currentCardinality;

    private int[] countArray;
    private CountList[] pastCountsArray;

    /**
     * Create a new HyperLogLog by specifying the relative error and confidence of answers
     * being within the error margin.
     * Based on the relative error the array size is calculated.
     *
     * @param relativeError is a number in the range (0, 1)
     * @param confidence is a value out of 0.65, 0.95, 0.99
     */
    public HyperLogLog(double relativeError, double confidence) {
        this.relativeError = relativeError;
        this.confidence = confidence;

//      relativeError = standardError / sqrt(noOfBuckets) = > noOfBuckets = (standardError / relativeError) ^ 2
        noOfBuckets = (int) Math.ceil(Math.pow(standardError / relativeError, 2));

//      noOfBuckets = 2 ^ lengthOfBucketId = >  lengthOfBucketId = log2(noOfBuckets) = ln(noOfBuckets) / ln(2)
        lengthOfBucketId = (int) Math.ceil(Math.log(noOfBuckets) / Math.log(2));

        noOfBuckets = (1 << lengthOfBucketId);

//      HyperLogLog estimations valid only when at least 16 buckets are used.
//      Therefore the minimum length of bucket id = 4
        if (lengthOfBucketId < 4) {
            throw new IllegalArgumentException("a higher relative error of " + relativeError +
                    " cannot be achieved");
        }

        countArray = new int[noOfBuckets]; //TODO : if enabled
        pastCountsArray = new CountList[noOfBuckets];
        for (int i = 0; i < noOfBuckets; i++) {
            pastCountsArray[i] = new CountList();
        }

        estimationFactor = getEstimationFactor(lengthOfBucketId, noOfBuckets);

        harmonicCountSum = noOfBuckets;
        noOfZeroBuckets = noOfBuckets;

    }

    /**
     * Calculate the cardinality(number of unique items in a set)
     * by calculating the harmonic mean of the counts in the buckets.
     * Check for the upper and lower bounds to modify the estimation.
     *
     * n - number of buckets
     * ci - count of the i th bucket
     *
     * harmonic count mean = n / ((1/2)^c1 + (1/2)^c2 + ... + (1/2)^cn)
     *
     * estimated cardinality = n * estimationFactor * harmonicCountMean
     */
    private void calculateCardinality() {

        double harmonicCountMean;
        long estimatedCardinality;
        long cardinality;

        harmonicCountMean = noOfBuckets / harmonicCountSum;

//      calculate the estimated cardinality
        estimatedCardinality = (long) Math.ceil(noOfBuckets * estimationFactor * harmonicCountMean);

//      if the estimate E is less than 2.5 * 32 and there are buckets with max-leading-zero count of zero,
//      then instead return −32⋅log(V/32), where V is the number of buckets with max-leading-zero count = 0.
//      threshold of 2.5x comes from the recommended load factor
        if ((estimatedCardinality < 2.5 * noOfBuckets) && noOfZeroBuckets > 0) {
            cardinality = (long) (-noOfBuckets * Math.log((double) noOfZeroBuckets / noOfBuckets));
        } else if (estimatedCardinality > (pow2of32 / 30.0)) {
//      if E > 2 ^ (32) / 30 : return −2 ^ (32) * log(1 − E / 2 ^ (32))
            cardinality = (long) Math.ceil(-(pow2of32 * Math.log(1 - (estimatedCardinality / (pow2of32)))));
        } else {
            cardinality = estimatedCardinality;
        }
        this.currentCardinality = cardinality;
    }

    /**
     * @return the current cardinality value
     */
    public long getCardinality() {
        return this.currentCardinality;
    }

    /**
     * Calculate the confidence interval for the current cardinality.
     * The confidence values can be one value out of 0.65, 0.95, 0.99.
     * @return an long array which contain the lower bound and the upper bound of the confidence interval
     * e.g. - {310, 350} for the cardinality of 330
     */
    public long[] getConfidenceInterval() {
        long[] confidenceInterval = new long[2];

//      sigma = relative error
        if (confidence == 0.65) {//      65% sure the answer in the range of sigma
            confidenceInterval[0] = (long) Math.floor(currentCardinality - (currentCardinality * relativeError * 0.5));
            confidenceInterval[1] = (long) Math.ceil(currentCardinality + (currentCardinality * relativeError * 0.5));
        }
        else if (confidence == 0.95) {//      95% sure the answer in the range of (2 * sigma)
            confidenceInterval[0] = (long) Math.floor(currentCardinality - (currentCardinality * relativeError));
            confidenceInterval[1] = (long) Math.ceil(currentCardinality + (currentCardinality * relativeError));
        }
        else if (confidence == 0.99) {//      99% sure the answer in the range of (3 * sigma)
            confidenceInterval[0] = (long) Math.floor(currentCardinality - (currentCardinality * relativeError * 1.5));
            confidenceInterval[1] = (long) Math.ceil(currentCardinality + (currentCardinality * relativeError* 1.5));
        }
        return confidenceInterval;
    }

    /**
     * Adds a new item to the array by hashing and increasing the count of relevant buckets
     *
     * @param item
     */
    public void addItem(E item) {
        int hash = getHashValue(item);

//      Shift all the bits to right till only the bucket ID is left
        int bucketId = hash >>> (Integer.SIZE - lengthOfBucketId);

//      Shift all the bits to left till the bucket id is removed
        int remainingValue = hash << lengthOfBucketId;

        int newLeadingZeroCount = Integer.numberOfLeadingZeros(remainingValue) + 1;

//      update the value in the  bucket
        int currentLeadingZeroCount = countArray[bucketId];
        pastCountsArray[bucketId].add(newLeadingZeroCount);
        if (currentLeadingZeroCount < newLeadingZeroCount) {

            harmonicCountSum = harmonicCountSum - (1.0 / (1L << currentLeadingZeroCount))
                    + (1.0 / (1L << newLeadingZeroCount));

            if (currentLeadingZeroCount == 0) {
                noOfZeroBuckets--;
            }
            if (newLeadingZeroCount == 0) {
                noOfZeroBuckets++;
            }

            countArray[bucketId] = newLeadingZeroCount;

            calculateCardinality();
        }
    }

    /**
     * Removes the given item from the array and restore the cardinality value by using the previous count.
     *
     * @param item
     */
    public void removeItem(E item) { //ToDO
        int hash = getHashValue(item);

//      Shift all the bits to right till only the bucket ID is left
        int bucketId = hash >>> (Integer.SIZE - lengthOfBucketId);

//      Shift all the bits to left till the bucket id is removed
        int remainingValue = hash << lengthOfBucketId;

        int currentLeadingZeroCount = Integer.numberOfLeadingZeros(remainingValue) + 1;

        int newLeadingZeroCount = pastCountsArray[bucketId].remove(currentLeadingZeroCount);
        int oldLeadingZeroCount = countArray[bucketId];

//      check the next maximum leading zero count
        if (newLeadingZeroCount >= 0) {

            harmonicCountSum = harmonicCountSum - (1.0 / (1L << oldLeadingZeroCount))
                    + (1.0 / (1L << newLeadingZeroCount));
            if (oldLeadingZeroCount == 0) {
                noOfZeroBuckets--;
            }
            if (newLeadingZeroCount == 0) {
                noOfZeroBuckets++;
            }

            countArray[bucketId] = newLeadingZeroCount;

            calculateCardinality();
        }

    }

    /**
     * Compute an integer hash value for a given value
     *
     * @param value to be hashed
     * @return integer hash value
     */
    public int getHashValue(Object value) {
        return MurmurHash.hash(value);
    }

    /**
     * Calculate the {@code estimationFactor} based on the length of bucket id and number of buckets
     *
     * @param lengthOfBucketId is the length of bucket id
     * @param noOfBuckets      is the number of buckets
     * @return {@code estimationFactor} //TODO : proven values from research paper
     */
    private double getEstimationFactor(int lengthOfBucketId, int noOfBuckets) {
        switch (lengthOfBucketId) {
            case 4:
                return 0.673;
            case 5:
                return 0.697;
            case 6:
                return 0.709;
            default:
                return (0.7213 / (1 + 1.079 / noOfBuckets));
        }
    }

    public double getRelativeError() {
        return relativeError;
    } //TODO : remove unused

    public double getConfidence() { return confidence; }
}


