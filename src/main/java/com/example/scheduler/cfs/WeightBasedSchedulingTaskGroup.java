package com.example.scheduler.cfs;

import com.example.scheduler.SimTask;

/** A group that schedules its TaskEntities based on weight. */
final class WeightBasedSchedulingTaskGroup extends TaskGroup {

  WeightBasedSchedulingTaskGroup(TaskManager manager, TaskGroup parent, long weight, int numThreads) {
    super(manager, parent, weight, numThreads);
  }

  @Override
  protected TaskEntity newTaskEntity(int thread, SimTask task, long weight) {
    return new WeightBasedTaskEntity(getManager(), getEntity(thread), task, weight);
  }

  @Override
  protected boolean isWeightBased() {
    return true;
  }
}
