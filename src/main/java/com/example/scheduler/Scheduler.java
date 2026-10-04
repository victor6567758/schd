package com.example.scheduler;

/**
 * Common contract for the round-robin and CFS schedulers.
 *
 * Mirrors a single-threaded executor loop: dequeue the next task, run it for one slice, put it
 * back if it isn't done yet.
 */
public interface Scheduler {

  /** Adds a brand-new runnable task to the scheduler. */
  void submit(SimTask task);

  /** @return true if there are no runnable tasks left. */
  boolean isEmpty();

  /**
   * Picks the next task, runs it for one slice, and re-enqueues it if it isn't finished.
   *
   * @return the id of the task that ran, or null if the scheduler was empty.
   */
  String runNext();

  /**
   * @return the number of runnable tasks currently held. Used by LoadBalancer to compare load
   *     across per-thread schedulers - distinct from CFS's internal weight-sum "load", this is a
   *     plain task count.
   */
  int size();

  /**
   * Removes and returns one runnable task without executing it, for migration to another
   * scheduler/thread, or null if none is available to move.
   */
  SimTask steal();
}
