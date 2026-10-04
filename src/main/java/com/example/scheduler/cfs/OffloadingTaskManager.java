package com.example.scheduler.cfs;

import com.example.scheduler.Observer;
import java.util.PriorityQueue;
import java.util.function.DoubleSupplier;

/**
 * Offloads all re-enqueue operations to a single ReEnqueueThread, so re-enqueuing threads (e.g.
 * RPC event threads reacting to a task unblocking) don't queue up one-at-a-time on the manager's
 * mutex themselves - one thread doing them all in a batch has the same effect, with far less
 * lock contention, and lets the caller thread move on immediately.
 */
public final class OffloadingTaskManager extends TaskManager {

  private final DeferredTaskQueue deferredTaskQueue;
  private final ReEnqueueThread reEnqueueThread;
  private volatile boolean offloadEnabled;

  public OffloadingTaskManager(
      int numThreads,
      int numExtensionThreads,
      Observer observer,
      boolean onIdleLoadShed,
      boolean rescheduleOnUnblock,
      boolean eagerWorkShed,
      DoubleSupplier cpuLoadSupplier,
      DoubleSupplier systemLoadAverageSupplier,
      boolean extensionPoolEnabled) {
    super(
        numThreads,
        numExtensionThreads,
        observer,
        onIdleLoadShed,
        rescheduleOnUnblock,
        eagerWorkShed,
        cpuLoadSupplier,
        systemLoadAverageSupplier,
        extensionPoolEnabled);
    this.deferredTaskQueue = new DeferredTaskQueue(this::reEnqueueHandler);
    this.offloadEnabled = true;
    this.reEnqueueThread = new ReEnqueueThread(this);
    this.reEnqueueThread.start();
  }

  void processBulkEnqueue(long waitTimeMs) throws InterruptedException {
    deferredTaskQueue.processLoop(waitTimeMs);
  }

  @Override
  void reEnqueueTaskEntity(TaskEntity entity) {
    if (offloadEnabled) {
      deferredTaskQueue.nonBlockingAdd(entity);
    } else {
      super.reEnqueueTaskEntity(entity);
    }
  }

  public void setOffloadEnabled(boolean offloadEnabled) {
    this.offloadEnabled = offloadEnabled;
  }

  private void reEnqueueHandler(PriorityQueue<TaskEntity> entityQueue) {
    while (!entityQueue.isEmpty()) {
      TaskEntity entity = entityQueue.poll();
      super.reEnqueueTaskEntity(entity);
    }
  }

  @Override
  public void close() {
    reEnqueueThread.close();
    // stopping the re-enqueue thread doesn't release the extension pool's resources on its own,
    // so release those too.
    super.close();
  }
}
