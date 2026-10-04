package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.example.scheduler.SimTask;
import com.example.scheduler.TaskHandle;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Direct tests of DeferredTaskQueue: the batching mechanism behind OffloadingTaskManager.
 * Entities queued via DeferredTaskQueue.nonBlockingAdd are never handed to the re-enqueue handler
 * individually - they're collected and delivered as one batch the next time
 * DeferredTaskQueue.processLoop runs.
 */
public class DeferredTaskQueueTest {

  @Test
  public void queuedEntitiesAreDeliveredAsOneBatch() throws InterruptedException {
    AtomicReference<PriorityQueue<TaskEntity>> captured = new AtomicReference<>();
    DeferredTaskQueue queue = new DeferredTaskQueue(captured::set);

    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, false);
    TaskEntity e1 = (TaskEntity) group.addTask(new SimTask("a", 100), 1);
    TaskEntity e2 = (TaskEntity) group.addTask(new SimTask("b", 100), 1);

    queue.nonBlockingAdd(e1);
    queue.nonBlockingAdd(e2);

    queue.processLoop(1000);

    assertNotNull("the handler should have been invoked with a non-empty batch", captured.get());
    assertEquals(2, captured.get().size());
  }

  @Test
  public void theHandlerIsNeverInvokedWhenNothingWasQueued() throws InterruptedException {
    boolean[] called = {false};
    DeferredTaskQueue queue = new DeferredTaskQueue(pq -> called[0] = true);

    long start = System.currentTimeMillis();
    queue.processLoop(50); // waits up to 50ms for something to arrive

    assertFalse(called[0]);
    assertTrue(
        "should have actually waited close to the timeout, not returned instantly",
        System.currentTimeMillis() - start >= 40);
  }

  @Test
  public void addingWhileAWaitIsInProgressStillGetsPickedUp() throws InterruptedException {
    AtomicReference<PriorityQueue<TaskEntity>> captured = new AtomicReference<>();
    DeferredTaskQueue queue = new DeferredTaskQueue(captured::set);

    TaskManager manager = new TaskManager(1);
    TaskHandle handle = manager.addTask(new SimTask("t", 100), 1);
    TaskEntity entity = (TaskEntity) handle;

    Thread producer =
        new Thread(
            () -> {
              try {
                Thread.sleep(20);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              queue.nonBlockingAdd(entity);
            });
    producer.start();

    queue.processLoop(2000); // long enough to still be waiting when the producer adds
    producer.join();

    assertNotNull(captured.get());
    assertEquals(1, captured.get().size());
  }
}
