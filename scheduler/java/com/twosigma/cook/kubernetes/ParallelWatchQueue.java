package com.twosigma.cook.kubernetes;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implement a processor that can process watch events from k8s in parallel.
 * Events are respresented as Runnable's (presmuably as clojures over some
 * underlying event state). Events on the same shard are processed sequentially.
 * Events on different shards are processed in parallel on the supplied
 * ExecutorService. Backpressure is implemented. When there are too many
 * outstanding events, submitting an event will block.
 */
public class ParallelWatchQueue {
    /**
     * Shard array
     */
    final ArrayList<Shard> shards;
    /**
     * Implements backpressure. Semaphore is grabbed whenever queueing.
     */
    final Semaphore queueSlotsLeft;
    final ExecutorService executor;
    final int shardCount;

    /**
     * Create a processor that can process watch events concurrently. It
     * guarantees that events on the same shard are processed in a linear
     * order. This code is multithread safe and can
     *
     * @param executor       The executor that has runnables submitted.
     * @param maxOutstanding How many events should we have queued up
     *                       before we backpressure and block submitting watch events.
     * @param maxShards      how many shards to use.
     */
    public ParallelWatchQueue(ExecutorService executor, int maxOutstanding, int maxShards) {
        this.executor = executor;
        // Immutable after construction.
        this.shards = new ArrayList<>(maxShards);
        this.shardCount = maxShards;
        this.queueSlotsLeft = new Semaphore(maxOutstanding, true);
        for (int ii = 0; ii < maxShards; ii++) {
            shards.add(new Shard());
        }
    }

    /**
     * Submit a given watch event, represented as a Runnable, on the given
     * shard number.
     * We guarantee that processing. is done asynchronously on a thread
     * such that events submitted in a happensBefore order on the same
     * shard number are processed in that same order.
     *
     * @param event    The event operation.
     * @param shardNum The shard number it is submitted on.
     * @throws InterruptedException
     */
    public void submitEvent(Runnable event, int shardNum) throws InterruptedException {
        // Block if we are backpressuring.
        queueSlotsLeft.acquire();
        shards.get(shardNum).submitEvent(event);
    }

    /**
     * Number of shards the workload is divided between.
     * @return shard count.
     */
    public int getShardCount() {
        return shardCount;
    }

    /**
     * Implements a single queue shard.
     */
    private class Shard {
        /* Used to lock adding events or processing events from a shard */
        private final Lock lock = new ReentrantLock();
        /**
         * Implements the actual queue of outstanding events
         */
        private final Deque<Runnable> deque = new ArrayDeque<Runnable>();

        /**
         * Process all queued events in this shard. This can be called
         * from multiple threads. It internally locks, allowing only one
         * thread to actually process this shard at a time.
         */
        void processDeque() {
            lock.lock();
            try {
                while (!deque.isEmpty()) {
                    var event = deque.removeFirst();
                    // Mark event as dequeed so we release any backpressure.
                    queueSlotsLeft.release();
                    event.run();
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Submit an event.
         * <p>
         * If two events are submitted with a happensBefore relationship the locks guarantee
         * that the events will be queued sequentially in the the queue.
         * <p>
         * Because processQueue locks on the same lock. Any events currently being processed
         * on this shard will be processed before the new event is submitted.
         * <p>
         * We have an invariant that whenever a deque has an event on it, there is at least
         * one runnable in the executorservice that will process this shard.
         *
         * @param event The runnable to run.
         */
        void submitEvent(Runnable event) {
            lock.lock();
            try {
                deque.addLast(event);
            } finally {
                lock.unlock();
            }
            executor.submit(this::processDeque);
        }
    }
}
