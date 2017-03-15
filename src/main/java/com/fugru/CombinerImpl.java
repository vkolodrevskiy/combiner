package com.fugru;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * {@link Combiner} implementation.
 *
 * @author vkolodrevskiy
 */
public class CombinerImpl<T> extends Combiner<T> {
    private final Logger log = LoggerFactory.getLogger(CombinerImpl.class);

    private final Set<BlockingQueueWrapper<T>> queues = new CopyOnWriteArraySet<>();

    // is used for stochastic selection algorithm,
    // stores max weight across input queues
    private AtomicReference<Double> maxWeight = new AtomicReference<>(0.0);

    // working thread that is gonna dispatch items from queues to SynchronousQueue
    private Thread thread;

    private final ReentrantLock mainLock = new ReentrantLock();

    public CombinerImpl(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
        start();
    }

    private void start() {
        thread = new Thread(() -> {
            BlockingQueueWrapper<T> q2read;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (queues.size() > 0) {
                        q2read = getQueueToRead();
                        q2read.updateEmptinessTimeout();
                        if (q2read.isTimedOut()) {
                            try {
                                removeInputQueue(q2read.getQueue());
                            } catch (CombinerException e) {
                                log.error("Unable to remove queue.", e);
                            }
                        } else {
                            if (!q2read.getQueue().isEmpty()) {
                                outputQueue.put(q2read.getQueue().take());
                            }
                        }
                    } else {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    log.info("Thread was interrupted.");
                    // Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        thread.start();
    }

    public void shutdown() {
        thread.interrupt();
    }

    private BlockingQueueWrapper<T> getQueueToRead() {
        List<BlockingQueueWrapper<T>> copyList = queues.stream().collect(Collectors.toCollection(ArrayList::new));
        return copyList.get(Utils.getStochasticIndex(copyList, maxWeight.get()));
    }

    @Override
    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws CombinerException {
        mainLock.lock();
        try {
            queues.add(new BlockingQueueWrapper<>(queue, priority, isEmptyTimeout, timeUnit));
            if (priority > maxWeight.get()) {
                maxWeight.set(priority);
            }
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {
        mainLock.lock();
        try {
            queues.remove(new BlockingQueueWrapper<>(queue));
            maxWeight.set(getMaxWeight());
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return queues.contains(new BlockingQueueWrapper<>(queue));
    }

    private double getMaxWeight() {
        double max = 0.0;
        for (BlockingQueueWrapper<T> queue : queues) {
            if (queue.getPriority() > max) {
                max = queue.getPriority();
            }
        }
        return max;
    }

    // for tests only
    public Set<BlockingQueueWrapper<T>> getQueues() {
        return queues;
    }
}
