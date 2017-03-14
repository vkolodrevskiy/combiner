package com.fugru;

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * {@link BlockingQueueWrapper} tests.
 *
 * @author vkolodrevskiy
 */
public class BlockingQueueWrapperTest {
    @Test
    public void isTimedEmptyQueueOut() throws Exception {
        BlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue<>();
        BlockingQueueWrapper qw = new BlockingQueueWrapper<>(blockingQueue, 0.5, 1, TimeUnit.MICROSECONDS);
        TimeUnit.MICROSECONDS.sleep(500);
        qw.updateEmptinessTimeout();
        assertTrue(qw.isTimedOut());
    }

    @Test
    public void isTimedOut() throws Exception {
        BlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue<>();
        blockingQueue.add(1);
        BlockingQueueWrapper qw = new BlockingQueueWrapper<>(blockingQueue, 0.5, 1, TimeUnit.MICROSECONDS);
        blockingQueue.take();
        qw.updateEmptinessTimeout();
        TimeUnit.MICROSECONDS.sleep(500);
        qw.updateEmptinessTimeout();
        assertTrue(qw.isTimedOut());
    }
}
