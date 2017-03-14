package com.fugru;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link CombinerImpl} test.
 *
 * @author vkolodrevskiy
 */
public class CombinerImplTest {
    private SynchronousQueue<Integer> synchronousQueue;
    private BlockingQueue<Integer> q1;
    private BlockingQueue<Integer> q2;
    private CombinerImpl<Integer> combiner;

    @Test
    public void AddAndRemoveInputQueue() throws Combiner.CombinerException, InterruptedException {
        combiner.addInputQueue(q1, 5.0, 50, TimeUnit.MICROSECONDS);
        combiner.addInputQueue(q2, 5.0, 50, TimeUnit.MICROSECONDS);
        assertEquals(2, combiner.getQueues().size());
        combiner.removeInputQueue(q1);
        combiner.removeInputQueue(q2);
        assertTrue(combiner.getQueues().isEmpty());
    }

    @Test
    public void hasInputQueue() throws Combiner.CombinerException {
        combiner.addInputQueue(q1, 5.0, 50, TimeUnit.MICROSECONDS);
        combiner.addInputQueue(q2, 5.0, 50, TimeUnit.MICROSECONDS);
        combiner.addInputQueue(q2, 5.0, 50, TimeUnit.MICROSECONDS);
        combiner.addInputQueue(q2, 5.0, 50, TimeUnit.MICROSECONDS);
        assertEquals(2, combiner.getQueues().size());
        assertTrue(combiner.hasInputQueue(q1));
        assertTrue(combiner.hasInputQueue(q2));
    }

    /**
     * Test that queue is gonna be evicted after timeout.
     */
    @Test
    public void queueTimeout1() throws Exception {
        combiner.addInputQueue(q1, 8.0, 80, TimeUnit.MICROSECONDS);
        combiner.addInputQueue(q2, 2.0, 20, TimeUnit.MICROSECONDS);

        assertFalse(combiner.getQueues().isEmpty());
        TimeUnit.SECONDS.sleep(2);

        assertTrue(combiner.getQueues().isEmpty());
    }

    @Test
    public void queueTimeout2() throws Exception {
        combiner.addInputQueue(q1, 8.0, 80, TimeUnit.MICROSECONDS);
        combiner.addInputQueue(q2, 2.0, 20, TimeUnit.MICROSECONDS);

        q1.add(10);
        q2.add(20);

        synchronousQueue.take();
        synchronousQueue.take();

        assertFalse(combiner.getQueues().isEmpty());
        TimeUnit.SECONDS.sleep(2);

        assertTrue(combiner.getQueues().isEmpty());
    }

    @Test
    public void combinerStochasticDistribution() throws Exception {
        for (int i = 0; i < 1000; i++) {
            q1.add(8);
            q2.add(2);
        }

        combiner.addInputQueue(q1, 8.0, 10, TimeUnit.SECONDS);
        combiner.addInputQueue(q2, 2.0, 10, TimeUnit.SECONDS);

        int[] counts = new int[2];
        for (int i = 0; i < 1000; i++) {
            int take = synchronousQueue.take();
            if (take == 8) {
                counts[0] ++;
            }
            if (take == 2) {
                counts[1] ++;
            }
        }

        double expected = counts[0] / 1000.0;
        double predicted = 8.0 / 10.0;
        double diff = Math.abs(expected - predicted);

        assertTrue(diff < 1e-1);
    }

    @Before
    public void setup() {
        synchronousQueue = new SynchronousQueue<>();
        combiner = new CombinerImpl<>(synchronousQueue);

        // create blocking queues
        q1 = new LinkedBlockingQueue<>();
        q2 = new LinkedBlockingQueue<>();
    }

    @After
    public void tearDown() {
        combiner.shutdown();
    }
}
