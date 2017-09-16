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
 * Calculate integer hash values for any type of object
 */
public final class MurmurHash {
    private MurmurHash() {
    }

    public static int hash(Object o) {
        if (o == null) {
            return 0;
        } else if (o instanceof Long) {
            return hashLong(((Long) o).longValue());
        } else if (o instanceof Integer) {
            return hashLong((long) ((Integer) o).intValue());
        } else if (o instanceof Double) {
            return hashLong(Double.doubleToRawLongBits(((Double) o).doubleValue()));
        } else if (o instanceof Float) {
            return hashLong((long) Float.floatToRawIntBits(((Float) o).floatValue()));
        } else if (o instanceof String) {
            return hash(((String) o).getBytes());
        } else {
            return hash(o.toString());
        }
    }

    public static int hashLong(long data) {
        int m = 1540483477;
        int r = 24;
        int h = 0;
        int k = (int) data * m;
        k ^= k >>> r;
        h = h ^ k * m;
        k = (int) (data >> 32) * m;
        k ^= k >>> r;
        h *= m;
        h ^= k * m;
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

}
