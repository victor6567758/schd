package com.example.scheduler;

/**
 * The basic load balancer:
 *
 * - when a runqueue is empty, steal a task from the busiest other runqueue
 * - only bother if that busiest runqueue has more than one runnable task
 * - never run periodically - only triggered when a thread actually goes idle
 *
 * Kept as a pure, synchronous decision (no threads involved) so it's trivial to unit test in
 * isolation; ThreadPoolExecutorSim is what actually calls this from real worker threads, with
 * the locking that requires.
 */
public final class LoadBalancer {

  private LoadBalancer() {}

  /**
   * @return the index into schedulers of the busiest one (excluding excludeIndex) with more than
   *     one runnable task, or -1 if none qualifies.
   */
  public static int pickBusiest(Scheduler[] schedulers, int excludeIndex) {
    int busiest = -1;
    int maxSize = 1;
    for (int i = 0; i < schedulers.length; i++) {
      if (i == excludeIndex) {
        continue;
      }
      int size = schedulers[i].size();
      if (size > maxSize) {
        maxSize = size;
        busiest = i;
      }
    }
    return busiest;
  }

  /**
   * If some other scheduler has more than one runnable task while schedulers[idleIndex] has none
   * of its own, steals one task from the busiest and submits it to the idle one.
   *
   * @return true if a task was moved.
   */
  public static boolean rebalance(Scheduler[] schedulers, int idleIndex) {
    int busiest = pickBusiest(schedulers, idleIndex);
    if (busiest < 0) {
      return false;
    }
    SimTask stolen = schedulers[busiest].steal();
    if (stolen == null) {
      return false;
    }
    schedulers[idleIndex].submit(stolen);
    return true;
  }
}
