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


//import org.wso2.extension.siddhi.execution.approximate.percentile.tdigest.TDigest                           ;

import com.tdunning.math.stats.TDigest;

import java.util.ArrayList;

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
//              accuracy = 4 * percentile * (1 - percentile) * certainty = 4 * percentile * (1 - percentile) / compression
        double compression = 4 * percentilePosition * (1 - percentilePosition) / accuracy;
//        if (compression < 1) {
//            throw new IllegalArgumentException("a lower accuracy of " + accuracy + " cannot be achieved");
//        }

        tDigest = TDigest.createDigest(compression);
    }

    @Override
    public void add(double newData) {
        tDigest.add(newData);
    }

    @Override
    public void add(ArrayList<Double> newData) {
        for(double d : newData) {
            tDigest.add(d);
        }
    }


    @Override
    public double getPercentile(double percentilePosition)
    {
        return tDigest.quantile(percentilePosition);
    }
}
