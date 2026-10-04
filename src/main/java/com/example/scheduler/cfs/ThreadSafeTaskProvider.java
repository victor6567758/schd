package com.example.scheduler.cfs;

/**
 * Thread-safe variant of DefaultTaskProvider, used by EagerTaskSheddingThreadMQ because an idle
 * thread can call getTaskToMigrate() concurrently with the owning thread calling
 * getTask()/taskDone(long).
 */
class ThreadSafeTaskProvider implements TaskProvider {

  private final RunQueue rq;
  private long lastSchedule;
  private TaskEntity current;

  ThreadSafeTaskProvider(RunQueue rq) {
    this.rq = rq;
  }

  @Override
  public void taskDone(long time) {
    TaskEntity previous = current;
    current = null;

    final long elapsed = time;
    final long duration = elapsed - lastSchedule;
    lastSchedule = elapsed;

    if (previous != null) {
      synchronized (this) {
        previous.prepareToStop(duration);

        if (!previous.isRunnable()) {
          previous.removeFromRunQueue();
        }
      }
    }
  }

  @Override
  public synchronized TaskEntity getTask() {
    if (rq.isEmpty()) {
      return null;
    }
    current = rq.getLeftMost();
    current.prepareToRun();
    return current;
  }

  @Override
  public synchronized TaskEntity getTaskToMigrate() {
    if (rq.isEmpty()) {
      return null;
    }
    TaskEntity task = rq.getLeftMost();
    task.removeFromRunQueue();
    return task;
  }

  @Override
  public void prepareToRelinquishThreadSlot() {
    // not wired up - see DefaultTaskProvider's note on this method.
  }
}
