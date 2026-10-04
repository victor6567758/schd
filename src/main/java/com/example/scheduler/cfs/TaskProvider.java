package com.example.scheduler.cfs;

/** Task provider from a thread's run queue: pops the next runnable task and accounts for the previous one's runtime. */
interface TaskProvider {

  /**
   * Marks the current task as done, updates its runtime, and puts it back in the run queue.
   *
   * @param time time spent running
   */
  void taskDone(long time);

  /** @return task to execute by priority; nullable */
  TaskEntity getTask();

  /** @return task that should be handed off to another thread; nullable */
  TaskEntity getTaskToMigrate();

  /**
   * Prepares to relinquish control of the run queue associated with this thread slot, so that
   * whatever takes over this slot can safely start using the run queue and group hierarchy.
   */
  void prepareToRelinquishThreadSlot();
}
