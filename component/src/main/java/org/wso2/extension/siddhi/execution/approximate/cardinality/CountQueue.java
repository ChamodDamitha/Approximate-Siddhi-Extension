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

import java.util.ArrayList;

public class CountQueue {

    ArrayList<Integer> counts;

    public CountQueue() {
        counts = new ArrayList<>();
    }

    /**
     * Add new value to the counts depending on previous value
     *
     * @param newValue
     * @return {@code true} if the newValue is added, {@code false} if the newValue is not added
     */
    public boolean add(int newValue) {
        if (counts.size() > 0) {
            for (int i = counts.size() - 1; i >= 0; i--) {
                if (newValue > counts.get(i)) {
                    counts.remove(i);
                } else {
                    break;
                }
            }
            if (counts.size() == 0 || newValue <= counts.get(counts.size() - 1)) {
                counts.add(newValue);
                return true;
            } else {
                return false;
            }
        } else {
            counts.add(newValue);
            return true;
        }
    }

    /**
     * Remove the given value from the counts
     *
     * @return the next value if the removed value is the first value,
     * -1 if no value is returned
     */
    public int remove(int value) {
        if (counts.size() > 0) {
            if (counts.get(0) == value) {
                counts.remove(0);
                if (counts.isEmpty()) {
                    return 0;
                } else {
                    return counts.get(0);
                }
            } else {
                return -1;
            }
        }
        return 0;
    }
}
