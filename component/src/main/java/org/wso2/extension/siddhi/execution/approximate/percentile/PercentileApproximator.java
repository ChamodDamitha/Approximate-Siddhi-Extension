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


package org.wso2.extension.siddhi.execution.approximate.percentile;


import org.wso2.extension.siddhi.execution.approximate.percentile.tdigest.TDigest;

/**
 * Calaculate percentiles using TDigest algorithm
 */
public class PercentileApproximator implements PercentileCalculator {
    private TDigest tDigest;

    @Override
    public void initialize(double certainty) {
        tDigest = TDigest.createDigest(1 / certainty);
    }

    @Override
    public void initialize(double percentilePosition, double accuracy) {
        tDigest = TDigest.createDigest(percentilePosition, accuracy);
    }

    @Override
    public void add(double newData) {
        tDigest.add(newData);
    }

    @Override
    public double getPercentile(double percentilePosition) {
        return tDigest.percentile(percentilePosition);
    }
}
