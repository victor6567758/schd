package com.example.scheduler;

/**
 * What the executor gets back for the task it should run next, and what a blocked task uses to
 * get itself back onto a runqueue once it unblocks (reEnqueue()).
 */
public interface TaskHandle {
  SimTask getTask();

  /** Called once getTask()'s state goes back to RUNNABLE after having been BLOCKED. */
  void reEnqueue();

  int getThread();

  int getCurrentTaskLoad();
}
