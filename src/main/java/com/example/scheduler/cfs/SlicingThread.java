package com.example.scheduler.cfs;

import com.example.scheduler.TaskHandle;
import com.example.scheduler.WakeUpListener;

/**
 * The actual OS thread that repeatedly asks its ThreadScheduler for the next task and runs it.
 *
 * Runs on a virtual tick counter instead of real System.nanoTime() for the elapsed-time
 * measurement ThreadMQ/ExtensionMQ need, so the underlying algorithm's behavior doesn't depend on
 * real wall-clock timing.
 */
final class SlicingThread extends Thread implements WakeUpListener, AutoCloseable {

  private final ThreadScheduler scheduler;
  private final long sliceGranularity;
  private final Object monitor = new Object();
  private volatile boolean shutdown;
  private long virtualClock;

  SlicingThread(int threadNum, TaskManager manager, long sliceGranularity) {
    super("slicing-thread-" + threadNum);
    this.sliceGranularity = sliceGranularity;
    this.scheduler = manager.getThreadScheduler(threadNum, this);
    setDaemon(true);
  }

  @Override
  public void run() {
    TaskHandle handle = scheduler.getTask(virtualClock);
    while (!shutdown) {
      if (handle == null) {
        synchronized (monitor) {
          if (shutdown) {
            return;
          }
          try {
            monitor.wait(5);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      } else {
        virtualClock += handle.getTask().runFor(sliceGranularity);
      }
      handle = scheduler.getTask(virtualClock);
    }
  }

  @Override
  public void wakeUpIfIdle() {
    synchronized (monitor) {
      monitor.notifyAll();
    }
  }

  @Override
  public void close() {
    shutdown = true;
    wakeUpIfIdle();
    try {
      join(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
