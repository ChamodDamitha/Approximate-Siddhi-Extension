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
     * @return
     */
    public boolean add(int newValue) {
        if (counts.size() > 0) {
            for (int i = counts.size() - 1; i >= 0; i--) {
                if (newValue > counts.get(i)) {
                    counts.remove(i);
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
