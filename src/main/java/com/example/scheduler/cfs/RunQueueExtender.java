package com.example.scheduler.cfs;

import com.example.scheduler.Observer;
import com.example.scheduler.WakeUpListener;
import java.util.function.DoubleSupplier;

/**
 * Extends/stretches the thread pool if there's sufficient CPU capacity, by borrowing extra
 * "extension" threads for one task at a time each. Used CPU capacity in the extension pool is
 * not added to the used shares for the task/group.
 *
 * The current CPU load and system load average are read from injected DoubleSuppliers rather
 * than real OS calls, so this is deterministic to test without touching the actual host's CPU.
 * The number of available processors is likewise a constructor parameter. Recovering from "high
 * load average" is checked inline on the next selectExtensionPool call rather than via a
 * dedicated background polling thread - same effect (the extension pool re-opens once load
 * average dips), no extra thread.
 */
final class RunQueueExtender implements AutoCloseable {

  private final double maxAllowedLoadAverage;
  private final double cpuLoadThreshold;
  private final DiffQueue<Integer> freeExtensionThreads = new DiffQueue<>();
  private final ExtensionMQ[] extensionMqs;
  private final int numThreads;
  private final int numExtensionThreads;
  private final TaskManager manager;
  private final DoubleSupplier cpuLoadSupplier;
  private final DoubleSupplier loadAverageSupplier;
  private volatile Observer observer;
  private volatile boolean enabled;
  private volatile boolean highLoadAverage;
  private int numExtensionsAllowed;

  RunQueueExtender(
      Observer observer,
      TaskManager manager,
      int numThreads,
      int numExtensionThreads,
      DoubleSupplier cpuLoadSupplier,
      DoubleSupplier loadAverageSupplier) {
    // cpuLoadAverageThreshold * availableProcessors; a fixed 0.5 threshold and this project's
    // usual small thread counts keep the numbers easy to reason about in tests.
    this.maxAllowedLoadAverage = 0.5 * Math.max(numThreads, 1);
    this.observer = observer;
    this.cpuLoadThreshold = 0.8;
    this.extensionMqs = new ExtensionMQ[numExtensionThreads];
    this.numThreads = numThreads;
    this.numExtensionThreads = numExtensionThreads;
    this.manager = manager;
    this.cpuLoadSupplier = cpuLoadSupplier == null ? () -> 0.0 : cpuLoadSupplier;
    this.loadAverageSupplier = loadAverageSupplier == null ? () -> 0.0 : loadAverageSupplier;
  }

  boolean enable() {
    if (numExtensionThreads <= 0) {
      return false;
    }
    enabled = true;
    return true;
  }

  boolean disable() {
    enabled = false;
    return true;
  }

  ThreadScheduler getTaskProvider(int threadNum, WakeUpListener listener) {
    final int extensionIdx = threadNum - numThreads;
    if (extensionIdx < 0 || extensionIdx >= numExtensionThreads) {
      throw new IllegalArgumentException(
          "Thread number must be in range between "
              + numThreads
              + " and "
              + (numThreads + numExtensionThreads));
    }
    if (extensionMqs[extensionIdx] == null) {
      extensionMqs[extensionIdx] = new ExtensionMQ(threadNum, this, listener);
      freeExtensionThreads.add(threadNum);
    }
    return extensionMqs[extensionIdx];
  }

  int selectExtensionPool(int selected) {
    if (!enabled) {
      return selected;
    }
    if (highLoadAverage) {
      // wait for load average to dip below 50% of 1 CPU or 2 points below max, whichever's higher
      final double loadAverageLowerBound = Math.max(maxAllowedLoadAverage - 2, 0.5);
      if (loadAverageSupplier.getAsDouble() < loadAverageLowerBound) {
        highLoadAverage = false;
      } else {
        observer.rejectedDueToHighLoadAverage();
        return selected;
      }
    }
    if (cpuLoadSupplier.getAsDouble() >= cpuLoadThreshold) {
      observer.rejectedDueToCpuBusy();
      numExtensionsAllowed = 0;
      return selected;
    }
    if (numExtensionsAllowed <= 0) {
      double currentLoadAverage = loadAverageSupplier.getAsDouble();
      if (currentLoadAverage < maxAllowedLoadAverage) {
        // do not migrate if the cpu load average was high in the last minute
        numExtensionsAllowed =
            Math.min(((int) (maxAllowedLoadAverage - currentLoadAverage) + 1), numExtensionThreads);
      } else {
        highLoadAverage = true;
        numExtensionsAllowed = 0;
        observer.rejectedDueToHighLoadAverage();
        return selected;
      }
    }
    if (numExtensionsAllowed > 0) {
      Integer selectedExtension = freeExtensionThreads.poll();
      if (selectedExtension != null) {
        numExtensionsAllowed--;
        return selectedExtension;
      }
      observer.rejectedDueToNoExtensionThreads();
    }
    return selected;
  }

  /** assumed to be protected under the manager mutex */
  void addToRunQueue(TaskEntity entity, int newThread) {
    final int extensionIdx = newThread - numThreads;
    assert extensionIdx >= 0 && extensionIdx < numExtensionThreads;
    extensionMqs[extensionIdx].add(entity);
  }

  boolean handOverTaskEntityIfBusy(TaskEntity entity) {
    if (highLoadAverage || cpuLoadSupplier.getAsDouble() >= cpuLoadThreshold) {
      manager.reEnqueueTaskEntity(entity);
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    // no background monitor thread to stop
  }

  void releaseThread(int threadNum, int iteration, boolean done) {
    manager.mutexLock();
    try {
      observer.addToExtensionThread(threadNum - numThreads, iteration, done);
    } finally {
      manager.mutexUnlock();
    }
    freeExtensionThreads.add(threadNum);
  }

  int getExtensionThreadCount() {
    return numExtensionThreads;
  }
}
