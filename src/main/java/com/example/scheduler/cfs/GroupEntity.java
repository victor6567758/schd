package com.example.scheduler.cfs;

import java.util.List;

/**
 * Group of entities. Holds its entities in a RunQueue and is part of a specific thread. A group
 * knows its TaskGroup and can access its siblings (group entities corresponding to other
 * threads) - this is what lets one TaskGroup span every thread while still competing locally,
 * thread by thread, against whatever else is on that thread's runqueue.
 */
class GroupEntity extends Entity {

  static final long MAX_GROUP_PENALTY = 100;

  /** group represented by this entity */
  private final TaskGroup taskGroup;

  /** runqueue owned by this group */
  private final RunQueue runQueue;

  private final int thread;

  private final boolean isRoot;

  /**
   * if a group is getting too much CPU share on another thread, we increase the groupPenalty to
   * compensate it in all other groups. This value is capped to MAX_GROUP_PENALTY.
   */
  private long groupPenalty = 1;

  /** Non root group constructor */
  GroupEntity(TaskGroup taskGroup, long weight, int thread) {
    super(taskGroup.getParent().getEntity(thread), weight);
    this.thread = thread;
    if (taskGroup == null) {
      throw new NullPointerException("Invalid task group: null");
    }
    this.taskGroup = taskGroup;
    this.runQueue = new RunQueue();
    taskGroup.addCRuntime(getParent().getRunQueue().getMinVRuntime());
    isRoot = false;
  }

  /** Root group constructor */
  GroupEntity(TaskGroup taskGroup, int thread) {
    super(1);
    this.thread = thread;
    if (taskGroup == null) {
      throw new NullPointerException("Invalid task group: null");
    }
    this.taskGroup = taskGroup;
    this.runQueue = new RunQueue();
    isRoot = true;
  }

  int getThread() {
    return thread;
  }

  /** @return RunQueue that holds this group's children */
  RunQueue getRunQueue() {
    return runQueue;
  }

  /** @return Group entity from the same TaskGroup for a specific thread */
  GroupEntity getSibling(int thread) {
    return taskGroup.getEntity(thread);
  }

  @Override
  GroupEntity asGroupEntity() {
    return this;
  }

  @Override
  TaskEntity getLeftMost() {
    return runQueue.getLeftMost();
  }

  long getLeftMostWeight() {
    return runQueue.getLeftMostWeight();
  }

  /**
   * Weight divided by groupPenalty, not multiplied: since the scheduling rule is "bigger
   * fairWeight means slower vruntime growth means more CPU" (a weight-2 task reliably gets about
   * twice a weight-1 task's CPU), multiplying by a penalty that's larger for whichever group
   * already has more cumulative cRuntime would make that group even more favored, not less.
   * Dividing is what actually compensates the group that's ahead.
   *
   * Returns a fraction rather than a whole number: with integer division, weight=1 and any
   * penalty of 2 or more truncates straight to 0, cancelling the correction for the most common
   * unit-weight case.
   */
  @Override
  double getFairWeight() {
    return getWeight() / (double) groupPenalty;
  }

  @Override
  void addToRunQueue() {
    // do not enqueue, non-root groups, more than once
    if (!isRoot && isDequeued()) {
      super.addToRunQueue();
    }
  }

  @Override
  void prepareToRun() {
    if (!isRoot) {
      if (runQueue.isEmpty()) {
        super.prepareToRun();
      } else {
        prepareToRunEnqueued();
      }
    }
  }

  @Override
  void prepareToStop(long duration) {
    if (!isRoot) {
      super.prepareToStop(duration);
    }
  }

  @Override
  void removeFromRunQueue() {
    // non-empty, non-root, groups shouldn't be removed
    if (!isRoot && runQueue.isEmpty()) {
      super.removeFromRunQueue();
    }
  }

  @Override
  void vRuntimeUpdated(long delta) {
    taskGroup.addCRuntime(delta);
  }

  private long getCRuntime() {
    return taskGroup.getCRuntime();
  }

  /**
   * Updates all sibling group entities' shares (those currently competing head-to-head on this
   * same thread's runqueue):
   *
   * compute the sum of all children's cRuntime (total)
   * share = total divided by the number of children groups
   * a group's penalty = ceil(that group's cRuntime / share), capped at MAX_GROUP_PENALTY
   * groups at or below the share keep a penalty of 1
   *
   * This is what makes group fairness correct across threads, not just within one thread's
   * runqueue: without it, a group whose tasks happen to land on more threads would get more
   * aggregate CPU than an equal-weight group confined to fewer threads, even though each
   * individual thread's local vruntime math looks fair in isolation. cRuntime is the group's
   * cumulative vruntime contribution summed across all threads, so comparing it across sibling
   * groups (all part of the same parent, all visible on this thread) catches that imbalance and
   * compensates for it through getFairWeight().
   *
   * Groups with cRuntime equal to 0 are ignored. This generally only happens when multiple
   * groups are added to a runqueue at the same time, before any of them has run once; it's safe
   * to ignore them since they also have vruntime 0 and should be scheduled soon anyway.
   *
   * Structural note: groupsAsList(runQueue.iterator()) only sees groups that are physically
   * still in this runqueue, and whether a group stays there while one of its own members is
   * running depends on the prepareToRun() branch above - a group with other still-runnable
   * members takes the prepareToRunEnqueued() path and stays (marked RUNNING_ENQUEUED), while a
   * group with none takes the plain prepareToRun() path and is removed (marked
   * RUNNING_DEQUEUED), same as an ordinary task. With only two sibling groups, a multi-task
   * group therefore gets compared against a single-task sibling only right after its own
   * member ran (the single-task one has since left the queue); the reverse can't happen. That's
   * enough of a one-sided opportunity to engage this correction that "does grouping more tasks
   * together get exactly the same total CPU as one task" is not a clean invariant of this
   * algorithm - two single-task groups split evenly, but a multi-task group compared against a
   * single-task one only holds approximately, and only once three or more groups are involved.
   */
  void updateShares() {
    // retrieve all group entities currently in the runQueue
    final List<GroupEntity> groups = Helpers.groupsAsList(runQueue.iterator());
    if (groups.size() < 2) {
      return; // not enough groups running in parallel
    }

    // cache groups' cRuntime, we don't want the total to change while we are updating the shares
    final long[] cRuntimes = new long[groups.size()];
    long cRuntimeSum = 0;
    int numNonZeros = 0;
    for (int i = 0; i < cRuntimes.length; i++) {
      long cRuntime = groups.get(i).getCRuntime();
      cRuntimes[i] = cRuntime;

      if (cRuntime > 0) {
        numNonZeros++;
        cRuntimeSum += cRuntime;
      }
    }

    if (numNonZeros < 2) { // no need to update the shares
      return;
    }

    // compute fair CPU share for each child. Each group should have run for a duration equal to
    // share
    double share = (double) cRuntimeSum / numNonZeros;

    // update group shares - at this point we know share > 0
    for (int i = 0; i < cRuntimes.length; i++) {
      long cRuntime = cRuntimes[i];
      if (cRuntime > share) {
        final double penalty = Math.ceil(cRuntime / share);
        groups.get(i).groupPenalty = (long) Math.min(MAX_GROUP_PENALTY, penalty);
      } else {
        groups.get(i).groupPenalty = 1;
      }
    }
  }

  long getGroupPenalty() {
    return groupPenalty;
  }
}
