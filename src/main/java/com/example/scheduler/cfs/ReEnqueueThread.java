package com.example.scheduler.cfs;

/**
 * A dedicated thread that drains OffloadingTaskManager's DeferredTaskQueue and completes the
 * re-enqueue of tasks offloaded by other threads.
 */
final class ReEnqueueThread extends Thread implements AutoCloseable {
  private static final int PAUSE_TIME_MS = 5000;
  private final OffloadingTaskManager manager;
  private volatile boolean isClosing;

  ReEnqueueThread(OffloadingTaskManager manager) {
    super("ReEnqueueThread");
    this.manager = manager;
  }

  @Override
  public void run() {
    while (!isClosing) {
      try {
        manager.processBulkEnqueue(PAUSE_TIME_MS);
      } catch (InterruptedException e) {
        // ignore - checked again by the loop condition
      }
    }
  }

  @Override
  public void close() {
    isClosing = true;
    interrupt();
  }
}
