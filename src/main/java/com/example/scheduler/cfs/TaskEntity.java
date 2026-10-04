package com.example.scheduler.cfs;

import com.example.scheduler.SimTask;
import com.example.scheduler.Task;
import com.example.scheduler.TaskHandle;

/** Schedulable entity that can be executed. */
class TaskEntity extends Entity implements TaskHandle {

  private final TaskManager manager;

  /** task that will be executed */
  private final SimTask task;

  // current task load on the thread on which this task is runnable. Has a valid value only when
  // a task is runnable. Need not be thread safe as it is always assumed to be read and written by
  // the thread that receives the handle to this entity.
  private int currentLoad;

  TaskEntity(TaskManager manager, GroupEntity parent, SimTask task, long weight) {
    super(parent, weight);
    if (manager == null) {
      throw new NullPointerException("null manager");
    }
    if (task == null) {
      throw new NullPointerException("Invalid task: null");
    }
    this.manager = manager;
    this.task = task;
    this.currentLoad = 0;
  }

  @Override
  public SimTask getTask() {
    return task;
  }

  boolean isRunnable() {
    return task.getState() == Task.State.RUNNABLE;
  }

  void setCurrentLoad(int load) {
    currentLoad = load;
  }

  @Override
  TaskEntity getLeftMost() {
    return this;
  }

  @Override
  public String toString() {
    return String.format(
        "%s, vRuntime = %d, load = %d, group_penalty = %d, num_migration = %d",
        getState(),
        getVRuntime(),
        getParent().getRunQueue().getLoad(),
        getParent().getGroupPenalty(),
        getNumMigrated());
  }

  protected boolean isWeightBased() {
    return false;
  }

  @Override
  public void reEnqueue() {
    manager.reEnqueueTaskEntity(this);
  }

  @Override
  public int getThread() {
    return getParent().getThread();
  }

  @Override
  public int getCurrentTaskLoad() {
    return currentLoad;
  }
}
