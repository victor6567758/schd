package com.example.scheduler.cfs;

import java.util.PriorityQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Queues up all "unblocked" tasks for offloaded re-enqueue back to the scheduler. Used to
 * offload re-enqueuing tasks from many fast, lightweight threads (e.g. RPC event threads) onto
 * one dedicated thread (ReEnqueueThread) - since re-enqueuing needs the manager's mutex, many
 * threads doing it one-at-a-time under that lock has the same effect as one thread doing them
 * all in sequence, so batching it onto one thread cuts contention without changing behavior.
 */
final class DeferredTaskQueue {
  // staging queue for tracking offloaded tasks
  private final BlockingDeque<TaskEntity> queue;
  // handler that handles the offloaded tasks
  private final Consumer<PriorityQueue<TaskEntity>> reEnqueueHandler;
  // priority queue to re-enqueue the offloaded tasks in priority order
  private final PriorityQueue<TaskEntity> pq;

  DeferredTaskQueue(Consumer<PriorityQueue<TaskEntity>> reEnqueueHandler) {
    this.queue = new LinkedBlockingDeque<>();
    this.reEnqueueHandler = reEnqueueHandler;
    this.pq = new PriorityQueue<>();
  }

  /** Queues up a task that needs to be re-enqueued back into scheduler queues. */
  void nonBlockingAdd(TaskEntity entity) {
    this.queue.add(entity);
  }

  /**
   * Re-enqueues all backed-up tasks that are "unblocked". Assumed to be done by a single thread to
   * avoid contention.
   */
  void processLoop(long waitTimeMs) throws InterruptedException {
    TaskEntity val = this.queue.poll(waitTimeMs, TimeUnit.MILLISECONDS);
    while (val != null) {
      pq.add(val);
      val = this.queue.poll();
    }

    if (!pq.isEmpty()) {
      reEnqueueHandler.accept(pq);
    }
  }
}
