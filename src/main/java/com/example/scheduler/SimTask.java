package com.example.scheduler;

/**
 * A schedulable unit of work for the round-robin / CFS teaching examples.
 *
 * A task has a fixed amount of work split into "ticks" and a weight (used only on the CFS side;
 * ignored by round-robin). It is executed in slices via runFor(), and can also go
 * block()/unblock() to simulate waiting on I/O - blocking removes it from its runqueue;
 * unblocking requires the caller to also call the TaskHandle.reEnqueue() it was given when
 * submitted.
 *
 * Each tick is backed by a real piece of code: the Runnable passed to the constructor is
 * executed once per tick, on whichever thread the scheduler runs the slice on. A task that
 * needs 1000 ticks therefore really runs its Runnable 1000 times, in as many slices as the
 * scheduler decides to give it. Tasks created without a Runnable simply have empty ticks, which
 * is enough for tests that only care about scheduling order.
 */
public final class SimTask implements Task {

  private final String id;
  private final long weight;
  private final boolean eligibleForExtensionPool;
  private final Runnable tickWork;
  private long remaining;
  private long consumed;
  private int timesScheduled;
  private State state = State.RUNNABLE;

  public SimTask(String id, long totalWork) {
    this(id, totalWork, 1);
  }

  public SimTask(String id, long totalWork, long weight) {
    this(id, totalWork, weight, false);
  }

  /** A task whose every tick executes tickWork, with default weight 1. */
  public SimTask(String id, long totalWork, Runnable tickWork) {
    this(id, totalWork, 1, false, tickWork);
  }

  /** A task whose every tick executes tickWork. */
  public SimTask(String id, long totalWork, long weight, Runnable tickWork) {
    this(id, totalWork, weight, false, tickWork);
  }

  public SimTask(String id, long totalWork, long weight, boolean eligibleForExtensionPool) {
    this(id, totalWork, weight, eligibleForExtensionPool, () -> {});
  }

  public SimTask(
      String id,
      long totalWork,
      long weight,
      boolean eligibleForExtensionPool,
      Runnable tickWork) {
    if (totalWork <= 0) {
      throw new IllegalArgumentException("totalWork must be > 0");
    }
    if (weight <= 0) {
      throw new IllegalArgumentException("weight must be > 0");
    }
    this.id = id;
    this.remaining = totalWork;
    this.weight = weight;
    if (tickWork == null) {
      throw new IllegalArgumentException("tickWork must not be null");
    }
    this.eligibleForExtensionPool = eligibleForExtensionPool;
    this.tickWork = tickWork;
  }

  /**
   * Runs this task for at most ticks, executing its real work once per tick, and returns
   * how many ticks it actually consumed. If the work throws, the ticks completed before the
   * failure stay counted and the exception propagates to the caller.
   */
  public long runFor(long ticks) {
    if (state != State.RUNNABLE) {
      throw new IllegalStateException("cannot run task " + id + " in state " + state);
    }
    long actual = Math.min(ticks, remaining);
    timesScheduled++;
    try {
      for (long i = 0; i < actual; i++) {
        tickWork.run();
        remaining--;
        consumed++;
      }
    } finally {
      if (remaining <= 0) {
        state = State.DONE;
      }
    }
    return actual;
  }

  /** Simulates the task blocking on I/O - it will be removed from its runqueue. */
  public void block() {
    if (state != State.RUNNABLE) {
      throw new IllegalStateException("cannot block task " + id + " in state " + state);
    }
    state = State.BLOCKED;
  }

  /** The task is runnable again - the caller must still call its TaskHandle.reEnqueue(). */
  public void unblock() {
    if (state != State.BLOCKED) {
      throw new IllegalStateException("cannot unblock task " + id + " in state " + state);
    }
    state = State.RUNNABLE;
  }

  @Override
  public State getState() {
    return state;
  }

  public String getId() {
    return id;
  }

  public long getWeight() {
    return weight;
  }

  public boolean isEligibleForExtensionPool() {
    return eligibleForExtensionPool;
  }

  public boolean isDone() {
    return state == State.DONE;
  }

  public long getConsumed() {
    return consumed;
  }

  public int getTimesScheduled() {
    return timesScheduled;
  }

  @Override
  public String toString() {
    return id
        + "(state="
        + state
        + ", consumed="
        + consumed
        + ", remaining="
        + remaining
        + ", weight="
        + weight
        + ")";
  }
}
