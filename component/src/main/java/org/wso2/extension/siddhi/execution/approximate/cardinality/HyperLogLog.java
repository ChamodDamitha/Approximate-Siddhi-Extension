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

/**
 * A probabilistic data structure to calculate cardinality of a set
 *
 * @param <E> is the type of objects in the set
 */
public class HyperLogLog<E> {

    private final double standardError = 1.04;
    private int noOfBuckets;
    private int lengthOfBucketId;
    private int[] countArray;
    private CountQueue[] pastCountsArray;

    private double estimationFactor;

    private double specifiedAccuracy;

    /**
     * Create a new HyperLogLog by specifying the accuracy
     * Based on the accuracy the array size is calculated
     *
     * @param accuracy is a number in the range (0, 1)
     */
    public HyperLogLog(double accuracy) {

        this.specifiedAccuracy = accuracy;

//      95% of time answer lie within [answer +- 2 * accuracy * answer]
        accuracy = specifiedAccuracy / 2;

//      accuracy = standardError / sqrt(noOfBuckets) = > noOfBuckets = (standardError / accuracy) ^ 2
        noOfBuckets = (int) Math.ceil(Math.pow(standardError / accuracy, 2));

        lengthOfBucketId = (int) Math.ceil(Math.log(noOfBuckets) / Math.log(2));

        noOfBuckets = (1 << lengthOfBucketId);

        if (lengthOfBucketId < 4) {
            throw new IllegalArgumentException("a higher error margin of " + accuracy + " cannot be achieved");
        }

        countArray = new int[noOfBuckets];
        pastCountsArray = new CountQueue[noOfBuckets];
        for (int i = 0; i < noOfBuckets; i++) {
            pastCountsArray[i] = new CountQueue();
        }

        estimationFactor = getEstimationFactor(lengthOfBucketId, noOfBuckets);
    }

    /**
     * Compute the accuracy using the count array size
     * accuracy = standardError / sqrt(noOfBuckets)
     *
     * @return the accuracy value
     */
    public double getAccuracy() {
        return (standardError / Math.sqrt(noOfBuckets));
    }

    /**
     * Calculate the cardinality(number of unique items in a set)
     * by calculating the harmonic mean of the counts in the buckets.
     * Check for the upper and lower bounds to modify the estimation.
     *
     * @return the cardinality value
     */
    public long getCardinality() {

        double harmonicCountSum = 0;
        double harmonicCountMean;
        int noOfZeroBuckets = 0;
        int estimatedCardinality;
        long count;
        long cardinality;

//      calculate harmonic mean of the bucket values
        for (int i = 0; i < noOfBuckets; i++) {
            count = countArray[i];
            harmonicCountSum += (1.0 / (1 << count));

            if (count == 0) {
                noOfZeroBuckets++;
            }
        }

        harmonicCountMean = noOfBuckets / harmonicCountSum;

//      calculate the estimated cardinality
        estimatedCardinality = (int) Math.ceil(noOfBuckets * estimationFactor * harmonicCountMean);

        final double pow2to32 = Math.pow(2, 32);

//      if the estimate E is less than 2.5 * 32 and there are buckets with max-leading-zero count of zero,
//      then instead return −32⋅log(V/32), where V is the number of buckets with max-leading-zero count = 0.
//      threshold of 2.5x comes from the recommended load factor for Linear Counting
        if ((estimatedCardinality < 2.5 * noOfBuckets) && noOfZeroBuckets > 0) {
            cardinality = (int) (-noOfBuckets * Math.log((double) noOfZeroBuckets / noOfBuckets));
        } else if (estimatedCardinality > (pow2to32 / 30.0)) {
            //       if E > 2 ^ (32) / 30 : return −2 ^ (32) * log(1 − E / 2 ^ (32))
            cardinality = (int) Math.ceil(-(pow2to32 * Math.log(1 - (estimatedCardinality / (pow2to32)))));
        } else {
            cardinality = estimatedCardinality;
        }
        return cardinality;
    }

    /**
     * Calculate the confidence interval for the cardinality
     *
     * @return an long array which contain the lower bound and the upper bound of the confidence interval
     * e.g. - {313, 350} for the cardinality of 320
     */
    public long[] getConfidenceInterval() {
        long cardinality = getCardinality();
        long[] confidenceInterval = new long[2];
        confidenceInterval[0] = (long) Math.floor(cardinality - (cardinality * specifiedAccuracy));
        confidenceInterval[1] = (long) Math.ceil(cardinality + (cardinality * specifiedAccuracy));
        return confidenceInterval;
    }

    /**
     * Adds a new item to the array by hashing and setting the count of relevant bucckets
     *
     * @param item
     */
    public void addItem(E item) {
        int hash = getHashValue(item);

//      Shift all the bits to right till only the bucket ID is left
        int bucketId = hash >>> (Integer.SIZE - lengthOfBucketId);

//      Shift all the bits to left till the bucket id is removed
        int remainingValue = hash << lengthOfBucketId;

        int noOfLeadingZeros = Integer.numberOfLeadingZeros(remainingValue) + 1;

        updateBucket(bucketId, noOfLeadingZeros);
    }

    /**
     * Removes the given item from the array and restore the cardinality value by using the previous count
     * @param item
     */
    public void removeItem(E item){
        int hash = getHashValue(item);

//      Shift all the bits to right till only the bucket ID is left
        int bucketId = hash >>> (Integer.SIZE - lengthOfBucketId);

//      Shift all the bits to left till the bucket id is removed
        int remainingValue = hash << lengthOfBucketId;

        int noOfLeadingZeros = Integer.numberOfLeadingZeros(remainingValue) + 1;

        int newLeadingZeroCount = pastCountsArray[bucketId].remove(noOfLeadingZeros);
        if(newLeadingZeroCount >= 0) {
            countArray[bucketId] = newLeadingZeroCount;
        }
    }

    /**
     * Update the zero count value in the relevant bucket if the given value is larger than the existing value
     *
     * @param index            is the bucket ID of the relevant bucket
     * @param leadingZeroCount is the new zero count
     * @return {@code true} if the bucket is updated, {@code false} if the bucket is not updated
     */
    private boolean updateBucket(int index, int leadingZeroCount) {
        long currentZeroCount = countArray[index];
        if (currentZeroCount < leadingZeroCount) {
            countArray[index] = leadingZeroCount;
            pastCountsArray[index].add(leadingZeroCount);
            return true;
        }
        return false;
    }


    /**
     * Compute an integer hash value for a given value
     *
     * @param value to be hashed
     * @return integer hash value
     */
    public int getHashValue(E value) {
        return MurmurHash.hash(value);
    }

    /**
     * Calculate the {@code estimationFactor} based on the length of bucket id and number of buckets
     *
     * @param lengthOfBucketId is the length of bucket id
     * @param noOfBuckets      is the number of buckets
     * @return {@code estimationFactor}
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
}


