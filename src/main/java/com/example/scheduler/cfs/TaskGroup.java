package com.example.scheduler.cfs;

import com.example.scheduler.SimTask;
import com.example.scheduler.TaskHandle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a group across all threads - one GroupEntity per thread, sharing this class's
 * cumulated virtual runtime (cRuntime) across all of them. This is what lets
 * GroupEntity.updateShares() compare a group's total CPU usage against its siblings even when
 * the group's own tasks are spread across several threads.
 */
class TaskGroup {

  /** parent group, null for root */
  private final TaskGroup parent;

  /** one entity per thread */
  private final GroupEntity[] entities;

  /** cumulated virtual runtime of the group over all threads */
  private final AtomicLong cRuntime = new AtomicLong();

  private final TaskManager manager;

  /** Root Group constructor */
  TaskGroup(TaskManager manager, int numThreads) {
    this.manager = manager;
    parent = null;
    entities = new GroupEntity[numThreads];
    for (int i = 0; i < entities.length; i++) {
      entities[i] = new GroupEntity(this, i);
    }
  }

  /** Non-root Group constructor */
  TaskGroup(TaskManager manager, TaskGroup parent, long weight, int numThreads) {
    this.manager = manager;
    if (parent == null) {
      throw new NullPointerException("parent shouldn't be null");
    }
    this.parent = parent;
    entities = new GroupEntity[numThreads];
    for (int i = 0; i < entities.length; i++) {
      entities[i] = new GroupEntity(this, weight, i);
    }
  }

  TaskManager getManager() {
    return manager;
  }

  TaskGroup getParent() {
    return parent;
  }

  void addCRuntime(long delta) {
    cRuntime.addAndGet(delta);
  }

  long getCRuntime() {
    return cRuntime.get();
  }

  GroupEntity getEntity(int thread) {
    return entities[thread];
  }

  /** Submits a task under this group; picks which thread it lands on via the manager. */
  public TaskHandle addTask(SimTask task, long weight) {
    manager.mutexLock();
    try {
      int thread =
          isWeightBased() ? manager.pickLeastBusyInitialWeighted(entities, weight) : manager.pickLeastBusy();
      TaskEntity se = newTaskEntity(thread, task, weight);
      manager.addTaskEntity(se);
      return se;
    } finally {
      manager.mutexUnlock();
    }
  }

  protected boolean isWeightBased() {
    return false;
  }

  protected TaskEntity newTaskEntity(int thread, SimTask task, long weight) {
    return new TaskEntity(manager, getEntity(thread), task, weight);
  }

  /**
   * Creates a sub-group of this group.
   *
   * @param weightBasedScheduler if true, tasks (and sub-groups) submitted to the new group are
   *     ordered by weight first, vruntime second - see WeightBasedTaskEntity.
   */
  public TaskGroup addGroup(long weight, boolean weightBasedScheduler) {
    if (!weightBasedScheduler) {
      return new TaskGroup(manager, this, weight, entities.length);
    }
    return new WeightBasedSchedulingTaskGroup(manager, this, weight, entities.length);
  }
}
