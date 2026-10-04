package com.example.scheduler.cfs;

/**
 * Task provider that picks the first item in the queue. Not thread safe - used when the
 * executing thread is the only one ever touching this runqueue.
 *
 * Time is an abstract virtual tick counter here rather than real nanoseconds, so it's used
 * directly with no unit conversion.
 */
class DefaultTaskProvider implements TaskProvider {

  /** runqueue corresponding to thread */
  private final RunQueue rq;

  /** last time getTask was called */
  private long lastSchedule;

  /** currently running entity */
  private TaskEntity current;

  DefaultTaskProvider(RunQueue rq) {
    this.rq = rq;
  }

  /**
   * Return the currently running task to the runQueue, or remove it completely if it's no longer
   * runnable.
   *
   * @param time elapsed virtual time since scheduling started
   */
  @Override
  public void taskDone(long time) {
    taskDoneLocal(time, false);
    current = null;
  }

  /** Select the next task to run. */
  @Override
  public TaskEntity getTask() {
    if (rq.isEmpty()) {
      return null;
    }
    current = rq.getLeftMost();

    current.prepareToRun();
    return current;
  }

  /** @return task that was next in the runqueue, in a DEQUEUED state, ready for migration */
  @Override
  public TaskEntity getTaskToMigrate() {
    if (rq.isEmpty()) {
      return null;
    }
    TaskEntity task = rq.getLeftMost();
    task.removeFromRunQueue();
    return task;
  }

  /**
   * Deprecates this provider: forces the currently running entity out of the run queue so a new
   * provider taking its place can safely pick up the next task.
   *
   * Not wired up to anything in this project - kept only so TaskProvider has a consistent shape;
   * treats "no additional virtual time elapsed" as a safe no-op default since nothing drives it
   * with a real clock reading.
   */
  @Override
  public void prepareToRelinquishThreadSlot() {
    taskDoneLocal(lastSchedule, true);
  }

  private void taskDoneLocal(long time, boolean force) {
    final long elapsed = time;
    final long duration = elapsed - lastSchedule;
    lastSchedule = elapsed;

    if (current != null) {
      current.prepareToStop(duration);

      if (!current.isRunnable() || force) {
        // we need to ensure its parent groups have a consistent state
        // only way to do it is to enqueue then dequeue no-longer-runnable tasks
        current.removeFromRunQueue();
      }
    }
  }
}
