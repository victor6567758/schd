package com.example.scheduler.cfs;

import com.example.scheduler.Observer;
import com.example.scheduler.Task;
import com.example.scheduler.TaskHandle;

/**
 * Collects basic per-thread scheduling stats. Not thread safe - assumed to be called with the
 * manager's mutex held.
 */
public final class BasicStatsObserver implements Observer {

  private final ThreadStats[] perThreadStats;
  private final ExtensionThreadStats[] perThreadExtensionStats;
  private int allCurrentActiveTasks;
  private int allPeakActiveTasks;
  private int cpuBusyRejections;
  private int poolFullRejections;
  private int loadAverageRejections;

  public BasicStatsObserver(int numThreads, int numExtensionThreads) {
    perThreadStats = new ThreadStats[numThreads];
    perThreadExtensionStats = new ExtensionThreadStats[numExtensionThreads];
    for (int i = 0; i < numThreads; i++) {
      perThreadStats[i] = new ThreadStats();
    }
    for (int i = 0; i < numExtensionThreads; i++) {
      perThreadExtensionStats[i] = new ExtensionThreadStats();
    }
  }

  @Override
  public void addTask(TaskHandle task, int thread) {
    perThreadStats[thread].totalTasks++;
    allCurrentActiveTasks++;
    if (allCurrentActiveTasks > allPeakActiveTasks) {
      allPeakActiveTasks = allCurrentActiveTasks;
    }
    // task is always added in runnable state
    perThreadStats[thread].incrementRunnable();
  }

  @Override
  public void switchTask(TaskHandle current, TaskHandle next, int thread) {
    if (next != null) {
      perThreadStats[thread].incrementRun();
    }
  }

  @Override
  public void dequeueTask(TaskHandle task, int thread) {
    perThreadStats[thread].runnableTasks--;
    if (task.getTask().getState() == Task.State.DONE) {
      allCurrentActiveTasks--;
    }
  }

  @Override
  public void enqueueTask(TaskHandle task, int oldThread, int newThread) {
    perThreadStats[newThread].incrementRunnable();
  }

  @Override
  public void rebalance(TaskHandle task, int srcThread, int dstThread) {
    perThreadStats[srcThread].runnableTasks--;
    perThreadStats[dstThread].incrementRunnable();
    perThreadStats[dstThread].numMigrationsIn++;
  }

  @Override
  public void workRequestRejected(int thread) {
    perThreadStats[thread].numWorkRequestsRejected++;
  }

  @Override
  public void addToExtensionThread(int extensionThreadNum, int numIterations, boolean done) {
    perThreadExtensionStats[extensionThreadNum].numRuns += numIterations;
    perThreadExtensionStats[extensionThreadNum].totalTasks++;
    if (done) {
      allCurrentActiveTasks--;
    }
  }

  @Override
  public void rejectedDueToCpuBusy() {
    cpuBusyRejections++;
  }

  @Override
  public void rejectedDueToHighLoadAverage() {
    loadAverageRejections++;
  }

  @Override
  public void rejectedDueToNoExtensionThreads() {
    poolFullRejections++;
  }

  public int getNumThreads() {
    return perThreadStats.length;
  }

  public int getTotalTasks(int thread) {
    return perThreadStats[thread].totalTasks;
  }

  public int getRunnableTasks(int thread) {
    return perThreadStats[thread].runnableTasks;
  }

  public int getPeakRunnableTasks(int thread) {
    return perThreadStats[thread].peakRunnableTasks;
  }

  public int getNumSchedule(int thread) {
    return perThreadStats[thread].numRuns;
  }

  public int getNumMigrationsIn(int thread) {
    return perThreadStats[thread].numMigrationsIn;
  }

  public int getNumWorkRequestsRejected(int thread) {
    return perThreadStats[thread].numWorkRequestsRejected;
  }

  public int getAllCurrentActiveTasks() {
    return allCurrentActiveTasks;
  }

  public int getAllPeakActiveTasks() {
    return allPeakActiveTasks;
  }

  public int getCpuBusyRejections() {
    return cpuBusyRejections;
  }

  public int getLoadAverageRejections() {
    return loadAverageRejections;
  }

  public int getPoolFullRejections() {
    return poolFullRejections;
  }

  public int getExtensionTotalTasks(int extensionThread) {
    return perThreadExtensionStats[extensionThread].totalTasks;
  }

  public int getExtensionNumRuns(int extensionThread) {
    return perThreadExtensionStats[extensionThread].numRuns;
  }

  private static final class ThreadStats {
    private int totalTasks;
    private int runnableTasks;
    private int peakRunnableTasks;
    private int numRuns;
    private int numMigrationsIn;
    private int numWorkRequestsRejected;

    private void incrementRunnable() {
      runnableTasks++;
      if (runnableTasks > peakRunnableTasks) {
        peakRunnableTasks = runnableTasks;
      }
    }

    private void incrementRun() {
      numRuns++;
    }
  }

  private static final class ExtensionThreadStats {
    private int totalTasks;
    private int numRuns;
  }
}
