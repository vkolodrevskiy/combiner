package com.fugru;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * {@link BlockingQueue} wrapper, stores queue metadata.
 * This metadata mainly is related to empty queue eviction mechanism.
 * TODO: think about WeakHashMap.
 *
 * @author vkolodrevskiy
 */
public class BlockingQueueWrapper<T> {

    private final BlockingQueue<T> queue;
    private final Double priority;

    // how long queue was empty
    private Duration emptyPeriod;
    // when did the emptiness start
    private LocalDateTime emptyStartTime;
    // how long this queue can remain empty.
    private final Duration isEmptyTimeout;

    public BlockingQueueWrapper(BlockingQueue<T> queue,
                                Double priority,
                                long isEmptyTimeout,
                                TimeUnit timeUnit) {
        this.queue = queue;
        this.priority = priority;
        this.isEmptyTimeout = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(isEmptyTimeout, timeUnit));
        this.emptyPeriod = Duration.ZERO;

        if (queue.isEmpty()) {
            this.emptyStartTime = LocalDateTime.now();
        }
        else {
            emptyStartTime = null;
        }
    }

    public BlockingQueueWrapper(BlockingQueue<T> queue) {
        this(queue, 0.0, 0, TimeUnit.NANOSECONDS);
    }

    public BlockingQueue<T> getQueue(){
        return this.queue;
    }

    public Double getPriority() {
        return priority;
    }

    @Override
    public int hashCode() {
        return queue.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        BlockingQueueWrapper that = (BlockingQueueWrapper) obj;
        return queue.equals(that.queue);
    }

    /**
     * Checks whether queue emptiness period is over.
     * @return  {@code true} if this queue has timed out.
     */
    public boolean isTimedOut() {
        return emptyPeriod.compareTo(isEmptyTimeout) > 0;
    }

    /**
     * Add time to a queue's empty timeout if the queue is empty.
     * If there is no empty start time this will set it.
     */
    public void updateEmptinessTimeout() {
        if (!queue.isEmpty()) {
            this.emptyPeriod = Duration.ZERO;
            this.emptyStartTime = null;
            return;
        }
        if (emptyStartTime == null) {
            this.emptyPeriod = Duration.ZERO;
            emptyStartTime = LocalDateTime.now();
        } else {
            Duration duration = Duration.between(emptyStartTime, LocalDateTime.now());
            emptyPeriod = emptyPeriod.plus(duration);
        }
    }
}
