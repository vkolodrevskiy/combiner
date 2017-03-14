package com.fugru;

import java.util.List;

/**
 * Utilities.
 *
 * @author vkolodrevskiy
 */
public class Utils {
    /**
     * Proportionate selection of queue based on its weight.
     * See https://en.wikipedia.org/wiki/Fitness_proportionate_selection
     *
     * @param queues collection of queues.
     * @param maxWeight max weight within given collection of queues.
     * @param <T>
     * @return index of selected queue in list.
     */
    public static <T> int getStochasticIndex(List<BlockingQueueWrapper<T>> queues, double maxWeight) {
        boolean notAccepted = true;
        int index = 0;
        while (notAccepted) {
            index = (int) (queues.size() * Math.random());
            if (Math.random() < queues.get(index).getPriority() / maxWeight) {
                notAccepted = false;
            }
        }

        return index;
    }
}
