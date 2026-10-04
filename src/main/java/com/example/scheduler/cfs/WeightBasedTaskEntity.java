package com.example.scheduler.cfs;

import com.example.scheduler.SimTask;

/**
 * Schedulable entity that is scheduled by task weight first - this is the strict-priority
 * variant: a higher-priority task (e.g. a UI-facing one) should never be preempted by a
 * lower-priority one. Within the same weight class, ties still break by vruntime, so tasks of
 * equal priority still get proportional fairness among themselves.
 */
final class WeightBasedTaskEntity extends TaskEntity {

  WeightBasedTaskEntity(TaskManager manager, GroupEntity parent, SimTask task, long weight) {
    super(manager, parent, task, weight);
  }

  @Override
  public int compareTo(Entity o) {
    // Tasks with highest weight are given least priority so that they are picked first from
    // the runqueue
    int compareWeight = -1 * Long.compare(getWeight(), o.getWeight());
    return (compareWeight != 0) ? compareWeight : super.compareTo(o);
  }

  @Override
  protected boolean isWeightBased() {
    return true;
  }
}
