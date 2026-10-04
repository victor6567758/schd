package com.example.scheduler.cfs;

/**
 * Scheduling entity that can be part of a runqueue - this project only ever schedules SimTask,
 * so there's no generic type parameter here. Manages its state and virtual runtime and
 * propagates most of its events to its parent.
 *
 * Non-root entities have the following general behaviour:
 *
 * an entity starts with a DEQUEUED state.
 * addToRunQueue(): DEQUEUED -> ENQUEUED, (affects group and task entities)
 *   entity is added to its runQueue, runQueue's load increases
 *                  RUNNING_DEQUEUED -> RUNNING_ENQUEUED (affects group entities only)
 *   entity is added to its runQueue, as a new task is runnable below in the hierarchy, no load increase
 * prepareToRun(): ENQUEUED -> RUNNING_DEQUEUED, (affects group and task entities)
 *   entity is temporarily removed from its runQueue
 *               : ENQUEUED -> RUNNING_ENQUEUED (affects group entities only)
 *   entity has other runnable entities, so not removed from its parent runQueue
 * prepareToStop(): RUNNING_DEQUEUED -> ENQUEUED, (affects group and task entities)
 *   vRuntime updated and entity is added back to its runQueue
 *                  RUNNING_ENQUEUED -> ENQUEUED (affects group entities only)
 *   vRuntime updated and entity is removed and added back to its runQueue
 * removeFromRunQueue(): ENQUEUED -> DEQUEUED, (affects group and task entities)
 *   entity no longer runnable (blocked or done), removed from its runQueue, runQueue's load decreases
 *                     : RUNNING_ENQUEUED -> RUNNING_DEQUEUED, (affects group entities only)
 *   entity has no more runnable tasks (except for currently running task), removed from its runQueue, load remains
 */
abstract class Entity implements Comparable<Entity> {

  enum State {
    DEQUEUED, // entity is part of a runQueue but is not enqueued, thus cannot be scheduled.
    ENQUEUED, // runnable entity, currently enqueued.
    RUNNING_DEQUEUED, // running entity, currently dequeued but still part of its run queue load.
    // Typical for currently running task entity and group entities in the hierarchy with
    // no other task entities.
    RUNNING_ENQUEUED // running entity enqueued and part of its run queue load.
    // Typical for group entities when there are 'other' runnable task entities in the hierarchy,
    // other than currently running task entity.
  }

  /** priority */
  private final long weight;

  /** CFS virtual runtime */
  private long vRuntime;

  /** entity current state */
  private State state;

  private long numMigrated;

  /** parent entity, null for root entities */
  private GroupEntity parent;

  Entity(GroupEntity parent, long weight) {
    if (weight <= 0) {
      throw new IllegalArgumentException("Invalid weight: " + weight);
    }
    this.weight = weight;
    if (parent == null) {
      throw new NullPointerException("parent cannot be null for non-root entities");
    }
    this.parent = parent;
    state = State.DEQUEUED;
    vRuntime = parent.getRunQueue().getMinVRuntime();
  }

  Entity(long weight) {
    if (weight <= 0) {
      throw new IllegalArgumentException("Invalid weight: " + weight);
    }
    this.weight = weight;
    parent = null;
    state = State.DEQUEUED;
  }

  /**
   * Traverses the group hierarchy starting from this node, selecting the leftmost group at each
   * level, until it reaches a task.
   */
  abstract TaskEntity getLeftMost();

  /** Allows "special" entities, like groups, to use special weight values. */
  double getFairWeight() {
    return weight;
  }

  /** DEQUEUED -> ENQUEUED, RUNNING_DEQUEUED -> RUNNING_ENQUEUED */
  void addToRunQueue() {
    assertState(isDequeued(), "addToRunQueue()");

    state = (state == State.RUNNING_DEQUEUED) ? State.RUNNING_ENQUEUED : State.ENQUEUED;
    parent.getRunQueue().enqueue(this);
    parent.addToRunQueue();
  }

  /** ENQUEUED -> RUNNING_DEQUEUED */
  void prepareToRun() {
    assertState(isEnqueued(), "prepareToRun()");

    state = State.RUNNING_DEQUEUED;
    parent.getRunQueue().removeRunning(this);

    parent.prepareToRun();
  }

  /** ENQUEUED -> RUNNING_ENQUEUED */
  void prepareToRunEnqueued() {
    assertState(isEnqueued(), "prepareToRunEnqueued()");

    state = State.RUNNING_ENQUEUED;
    parent.prepareToRun();
  }

  /** RUNNING -> ENQUEUED */
  void prepareToStop(long duration) {
    assertState(isRunning(), "prepareToStop()");

    if (state == State.RUNNING_ENQUEUED) {
      // if enqueued, dequeue so that priority queue is updated with new vruntime
      parent.getRunQueue().removeRunning(this);
    }
    updateVRuntime(duration);
    // put back into its runQueue
    parent.getRunQueue().putBack(this);
    state = State.ENQUEUED;

    parent.prepareToStop(duration);
  }

  /** ENQUEUED -> DEQUEUED, RUNNING_ENQUEUED -> RUNNING_DEQUEUED */
  void removeFromRunQueue() {
    if (!isEnqueued()) {
      return;
    }

    state = (state == State.RUNNING_ENQUEUED) ? State.RUNNING_DEQUEUED : State.DEQUEUED;
    parent.getRunQueue().dequeue(this);
    parent.removeFromRunQueue();
  }

  /** computes the new virtual runtime */
  private void updateVRuntime(long duration) {
    final double relativeWeight = parent.getRunQueue().getLoad() / getFairWeight();
    final long delta = (long) Math.ceil(duration * relativeWeight);
    vRuntime += delta;
    vRuntimeUpdated(delta);
  }

  /** used by groups to update their group cumulated runtime */
  void vRuntimeUpdated(long delta) {}

  /** Assign this entity to the owner's runqueue corresponding to a given thread */
  void migrateTo(int thread) {
    if (thread == parent.getThread()) {
      // no migration required as we chose the same thread
      return;
    }
    numMigrated++;
    vRuntime -= parent.getRunQueue().getMinVRuntime();
    parent = parent.getSibling(thread);
    vRuntime += parent.getRunQueue().getMinVRuntime();
  }

  @Override
  public int compareTo(Entity o) {
    return Long.compare(vRuntime, o.vRuntime);
  }

  /** @return the group entity instance corresponding to this entity, null if it's a task */
  GroupEntity asGroupEntity() {
    return null;
  }

  long getWeight() {
    return weight;
  }

  long getVRuntime() {
    return vRuntime;
  }

  long getNumMigrated() {
    return numMigrated;
  }

  GroupEntity getParent() {
    return parent;
  }

  State getState() {
    return state;
  }

  boolean isEnqueued() {
    return state == State.ENQUEUED || state == State.RUNNING_ENQUEUED;
  }

  boolean isRunning() {
    return state == State.RUNNING_DEQUEUED || state == State.RUNNING_ENQUEUED;
  }

  boolean isDequeued() {
    return state == State.DEQUEUED || state == State.RUNNING_DEQUEUED;
  }

  boolean isRunningEnqueued() {
    return state == State.RUNNING_ENQUEUED;
  }

  boolean isRunningDequeued() {
    return state == State.RUNNING_DEQUEUED;
  }

  private void assertState(boolean condition, String method) {
    if (!condition) {
      throw new IllegalStateException(method + " called on state " + state);
    }
  }
}
