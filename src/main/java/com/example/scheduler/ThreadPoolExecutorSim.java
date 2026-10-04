package com.example.scheduler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Minimal multi-threaded executor: one Scheduler per worker thread, new tasks assigned
 * round-robin, and LoadBalancer used whenever a thread goes idle. Works with either
 * RoundRobinScheduler or CfsScheduler since both just implement Scheduler.
 *
 * Each scheduler is only ever touched by its own worker thread, except for
 * Scheduler.size()/Scheduler.steal() during load balancing, which is why every access goes
 * through that thread's own lock - one lock at a time is held, so this can't deadlock.
 */
public final class ThreadPoolExecutorSim implements AutoCloseable {

  private final Scheduler[] schedulers;
  private final Object[] locks;
  private final Thread[] threads;
  private volatile boolean shutdown;
  private int nextAssign;
  private final AtomicInteger totalRunCount = new AtomicInteger();

  public ThreadPoolExecutorSim(int numThreads, Supplier<Scheduler> schedulerFactory) {
    if (numThreads <= 0) {
      throw new IllegalArgumentException("numThreads must be > 0");
    }
    schedulers = new Scheduler[numThreads];
    locks = new Object[numThreads];
    threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      schedulers[i] = schedulerFactory.get();
      locks[i] = new Object();
    }
    for (int i = 0; i < numThreads; i++) {
      int threadId = i;
      threads[i] = new Thread(() -> workerLoop(threadId), "sim-worker-" + i);
      threads[i].setDaemon(true);
    }
  }

  public void start() {
    for (Thread t : threads) {
      t.start();
    }
  }

  /** Assigns a new task to a thread round-robin. */
  public synchronized void submit(SimTask task) {
    int thread = nextAssign;
    nextAssign = (nextAssign + 1) % schedulers.length;
    synchronized (locks[thread]) {
      schedulers[thread].submit(task);
      locks[thread].notifyAll();
    }
  }

  public int getNumThreads() {
    return schedulers.length;
  }

  public int getTotalRunCount() {
    return totalRunCount.get();
  }

  private void workerLoop(int threadId) {
    Scheduler mine = schedulers[threadId];
    while (!shutdown) {
      boolean ran = false;
      synchronized (locks[threadId]) {
        if (!mine.isEmpty()) {
          if (mine.runNext() != null) {
            ran = true;
          }
        }
      }
      if (ran) {
        totalRunCount.incrementAndGet();
        continue;
      }
      if (tryStealWork(threadId)) {
        continue;
      }
      synchronized (locks[threadId]) {
        if (mine.isEmpty() && !shutdown) {
          try {
            // short timeout: also covers the case where a task arrived on another thread that
            // could now be stolen, without a dedicated cross-thread wakeup for that.
            locks[threadId].wait(5);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    }
  }

  /**
   * Never holds two thread locks at once (avoids deadlock): LoadBalancer.pickBusiest reads every
   * scheduler's size() without locking (a benign race - worst case we pick a slightly stale
   * "busiest" thread), then the actual steal and submit each lock only their own scheduler.
   */
  private boolean tryStealWork(int idleThread) {
    int busiest = LoadBalancer.pickBusiest(schedulers, idleThread);
    if (busiest < 0) {
      return false;
    }
    SimTask stolen;
    synchronized (locks[busiest]) {
      stolen = schedulers[busiest].steal();
    }
    if (stolen == null) {
      return false;
    }
    synchronized (locks[idleThread]) {
      schedulers[idleThread].submit(stolen);
    }
    return true;
  }

  @Override
  public void close() {
    shutdown = true;
    for (Object lock : locks) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
    for (Thread t : threads) {
      try {
        t.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
