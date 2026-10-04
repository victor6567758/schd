package com.example.scheduler.cfs;

import com.example.scheduler.Observer;
import com.example.scheduler.SimTask;
import com.example.scheduler.TaskHandle;
import com.example.scheduler.WakeUpListener;
import java.util.function.DoubleSupplier;
import java.util.function.IntFunction;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates N per-thread ThreadMQs: assigns new tasks to the least busy thread, re-enqueues
 * unblocked tasks (possibly migrating them), and handles idle-thread work stealing via
 * requestMoreWork. Optionally backed by a RunQueueExtender for a small pool of "overflow" threads
 * used only when the host has spare CPU capacity. Always uses a real ReentrantLock.
 *
 * Threads are created eagerly in the constructor rather than lazily on first use, which avoids a
 * whole class of null-checks.
 */
public class TaskManager {

  private final TaskGroup root;
  private final ThreadMQ[] mqs;
  private final int numThreads;
  private final boolean rescheduleOnUnblock;
  private final Observer observer;
  private final boolean[] threadMQLoadChanged;
  private final LoadManager loadMgr;
  private final ReentrantLock lock = new ReentrantLock();
  private final RunQueueExtender runQueueExtender;

  public TaskManager(int numThreads) {
    this(numThreads, false, false, false);
  }

  public TaskManager(
      int numThreads, boolean onIdleLoadShed, boolean rescheduleOnUnblock, boolean eagerWorkShed) {
    this(numThreads, Observer.DUMMY, onIdleLoadShed, rescheduleOnUnblock, eagerWorkShed);
  }

  public TaskManager(
      int numThreads,
      Observer observer,
      boolean onIdleLoadShed,
      boolean rescheduleOnUnblock,
      boolean eagerWorkShed) {
    this(numThreads, 0, observer, onIdleLoadShed, rescheduleOnUnblock, eagerWorkShed, null, null, false);
  }

  /**
   * @param numExtensionThreads size of the extension pool (0 disables it)
   * @param cpuLoadSupplier current system CPU load in [0,1], injectable so tests are deterministic
   * @param systemLoadAverageSupplier current 1-minute system load average, same reasoning
   */
  public TaskManager(
      int numThreads,
      int numExtensionThreads,
      Observer observer,
      boolean onIdleLoadShed,
      boolean rescheduleOnUnblock,
      boolean eagerWorkShed,
      DoubleSupplier cpuLoadSupplier,
      DoubleSupplier systemLoadAverageSupplier,
      boolean extensionPoolEnabled) {
    if (numThreads <= 0) {
      throw new IllegalArgumentException("Invalid numThreads: " + numThreads);
    }
    this.numThreads = numThreads;
    this.rescheduleOnUnblock = rescheduleOnUnblock;
    this.observer = observer == null ? Observer.DUMMY : observer;
    this.threadMQLoadChanged = new boolean[numThreads];
    this.loadMgr = new LoadManager(numThreads);
    this.root = new TaskGroup(this, numThreads);
    this.mqs = new ThreadMQ[numThreads];

    WakeUpListener noop = () -> {};
    for (int i = 0; i < numThreads; i++) {
      mqs[i] =
          eagerWorkShed
              ? new EagerTaskSheddingThreadMQ(this, root.getEntity(i), noop, onIdleLoadShed)
              : new ThreadMQ(this, root.getEntity(i), noop, onIdleLoadShed);
      threadMQLoadChanged[i] = true;
    }

    if (numExtensionThreads > 0) {
      this.runQueueExtender =
          new RunQueueExtender(
              this.observer,
              this,
              numThreads,
              numExtensionThreads,
              cpuLoadSupplier,
              systemLoadAverageSupplier);
      if (extensionPoolEnabled) {
        runQueueExtender.enable();
      }
    } else {
      this.runQueueExtender = null;
    }
  }

  /** Replaces the no-op wake-up listener installed in the constructor for a real one. */
  public ThreadScheduler getThreadScheduler(int thread, WakeUpListener listener) {
    if (thread >= numThreads) {
      if (runQueueExtender == null) {
        throw new IllegalStateException("Extension pool is not enabled");
      }
      return runQueueExtender.getTaskProvider(thread, listener);
    }
    mqs[thread].setWakeUpListener(listener);
    return mqs[thread];
  }

  public int getNumThreads() {
    return numThreads;
  }

  /** Submits a brand-new, ungrouped task directly under the root group. */
  public TaskHandle addTask(SimTask task, long weight) {
    return root.addTask(task, weight);
  }

  /** Creates a new scheduling group under the root. */
  public TaskGroup newGroup(long weight, boolean weightBasedScheduler) {
    return root.addGroup(weight, weightBasedScheduler);
  }

  void reEnqueueTaskEntity(TaskEntity entity) {
    mutexLock();
    try {
      int currentThread = entity.getThread();
      if (rescheduleOnUnblock) {
        int newThread = entity.isWeightBased() ? pickLeastBusyWeighted(entity) : pickLeastBusy();
        if (newThread >= numThreads) {
          runInExtensionPool(entity, newThread);
        } else {
          entity.migrateTo(newThread);
          reEnqueue(entity, currentThread, newThread);
        }
      } else {
        // no reschedule-on-unblock ==> keep the same thread of execution
        reEnqueue(entity, currentThread, currentThread);
      }
    } finally {
      mutexUnlock();
    }
  }

  private void runInExtensionPool(TaskEntity entity, int newThread) {
    if (runQueueExtender == null) {
      throw new IllegalStateException("Unexpected call to run in a new thread");
    }
    runQueueExtender.addToRunQueue(entity, newThread);
  }

  /** Called with the manager's mutex held. */
  void addTaskEntity(TaskEntity task) {
    final int thread = task.getThread();
    observer.addTask(task, thread);
    threadMQLoadChanged[thread] = true;
    mqs[thread].add(task);
  }

  /**
   * Tries to add a runnable task to a given run queue: picks a task from the thread with the most
   * ready-to-run tasks (whether staged or actually running).
   */
  void requestMoreWork(int thread) {
    ThreadMQ src = null;
    long max = 0;
    for (ThreadMQ mq : mqs) {
      if (mq.getThread() != thread) {
        long load = mq.getLoad();
        if (load > max) {
          max = load;
          src = mq;
        }
      }
    }

    // a max of 1 implies that all threads have at most one task, and they're running (or about
    // to run) that task
    if (max <= 1) {
      observer.workRequestRejected(thread);
    } else {
      threadMQLoadChanged[src.getThread()] = true;
      src.requestWork(thread);
    }
  }

  void acceptWorkMigration(TaskEntity task, int src, int dst) {
    assert !task.isRunning();
    task.migrateTo(dst);
    mqs[dst].add(task);
    threadMQLoadChanged[src] = true;
    threadMQLoadChanged[dst] = true;
    observer.rebalance(task, src, dst);
  }

  void rejectTaskMigration(int thread) {
    // wake up the thread anyway, so it can request work from another thread
    mqs[thread].wakeUp();
  }

  void reEnqueue(TaskEntity task, int currentThread, int newThread) {
    threadMQLoadChanged[newThread] = true;
    mqs[newThread].add(task);
    observer.enqueueTask(task, currentThread, newThread);
  }

  /** @return threadId of the least busy runQueue */
  int pickLeastBusy() {
    long min = Long.MAX_VALUE;
    int selected = 0;
    for (int i = 0; i < numThreads && min > 0; i++) {
      long load = mqs[i].getLoad();
      if (load < min) {
        min = load;
        selected = i;
      }
    }
    return selected;
  }

  /**
   * Picks the least loaded thread in terms of runnable tasks, based on a relative-weight
   * calculation against other heavy-weight tasks of the same group already runnable there. If the
   * current thread isn't loaded much, stays put to avoid a migration. Caller must hold the mutex.
   */
  int pickLeastBusyWeighted(TaskEntity thisEntity) {
    return pickLeastBusyWeighted(
        (x) -> thisEntity.getParent().getSibling(x),
        thisEntity.getWeight(),
        runQueueExtender != null && thisEntity.getTask().isEligibleForExtensionPool());
  }

  /** Same as pickLeastBusyWeighted(TaskEntity), for a task not yet in any runqueue. */
  int pickLeastBusyInitialWeighted(GroupEntity[] entities, long weight) {
    return pickLeastBusyWeighted((x) -> entities[x], weight, false);
  }

  void mutexLock() {
    lock.lock();
  }

  void mutexUnlock() {
    lock.unlock();
  }

  void dequeueTask(TaskEntity task, int threadId) {
    observer.dequeueTask(task, threadId);
    threadMQLoadChanged[threadId] = true;
  }

  void switchTask(TaskEntity current, TaskEntity next, int threadId) {
    observer.switchTask(current, next, threadId);
  }

  public int getNumRunnableTasks() {
    int runnableTasks = 0;
    for (ThreadMQ mq : mqs) {
      runnableTasks += mq.getNumTasks();
    }
    return runnableTasks;
  }

  public void close() {
    if (runQueueExtender != null) {
      runQueueExtender.close();
    }
  }

  public boolean enableOrDisableExtensionPool(boolean enabled) {
    if (runQueueExtender == null) {
      return false;
    }
    return enabled ? runQueueExtender.enable() : runQueueExtender.disable();
  }

  int chooseExtensionPool(int selected) {
    return runQueueExtender == null ? selected : runQueueExtender.selectExtensionPool(selected);
  }

  private int pickLeastBusyWeighted(
      IntFunction<GroupEntity> toTopWeightFunc, long weight, boolean isEligibleForExtensionPool) {
    // recompute the load of only those threads that changed since the previous call
    for (int i = 0; i < numThreads; i++) {
      if (threadMQLoadChanged[i]) {
        threadMQLoadChanged[i] = false;
        loadMgr.setNewLoad(i, mqs[i].getLoad());
      }
    }

    int selected = 0;
    long relativeWeightMin = Long.MAX_VALUE;
    boolean allRunQueuesLoaded = true;
    for (LoadManager.LoadEntry e : loadMgr.getMinimumLoad()) {
      long lowestLoad = e.getLoad();
      if (lowestLoad <= 0) {
        // short circuit if load is 0 or negative
        selected = e.getThread();
        allRunQueuesLoaded = false;
        break;
      }
      // flush staging if we can so relative weights in the run queue for this group are accurate
      mqs[e.getThread()].flushStaging();

      long nextRel = mqs[e.getThread()].getRelativeWeight(toTopWeightFunc.apply(e.getThread()), weight);
      if (nextRel < relativeWeightMin) {
        relativeWeightMin = nextRel;
        selected = e.getThread();
      }
    }
    if (allRunQueuesLoaded && isEligibleForExtensionPool) {
      selected = chooseExtensionPool(selected);
    }
    return selected;
  }
}
