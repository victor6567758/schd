package com.example.scheduler.cfs;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Collection of runnable entities ordered by their vruntime. Keeps track of the collection's
 * sum of weights (its "load"), and their minimum virtual runtime. A runqueue instance is tied to
 * a particular executing thread, and all its attributes are related to that particular thread.
 */
final class RunQueue {

  private final Queue<Entity> queue = new PriorityBlockingQueue<>();

  private long leftMostWeight;

  /** sum of all runnable entities' weights */
  private long load;

  private long minVRuntime;

  long getMinVRuntime() {
    return minVRuntime;
  }

  long getLoad() {
    return load;
  }

  boolean isEmpty() {
    return queue.isEmpty();
  }

  /** @return without removing from the runQueue, the entity with smallest vRuntime. Can be null */
  Entity peek() {
    return queue.peek();
  }

  /**
   * Traverses the group hierarchy, selecting the leftmost group at each level, until it reaches a
   * task.
   *
   * @return left most task entity. Cannot be null
   */
  TaskEntity getLeftMost() {
    if (isEmpty()) {
      throw new IllegalStateException("calling getLeftMost() on an empty runqueue");
    }
    return queue.peek().getLeftMost();
  }

  long getLeftMostWeight() {
    return leftMostWeight;
  }

  /** Adds an entity that was previously DEQUEUED to the runQueue. Updates the load accordingly. */
  void enqueue(Entity entity) {
    if (!entity.isEnqueued()) {
      throw new IllegalStateException("entity should be ENQUEUED but is " + entity.getState());
    }

    queue.add(entity);
    if (!entity.isRunningEnqueued()) {
      leftMostWeight = peek().getWeight();
      load += entity.getWeight();
    }
  }

  /** Adds an entity that was RUNNING back into the runQueue. Updates the min vRuntime. */
  void putBack(Entity entity) {
    queue.add(entity);
    minVRuntime = Math.max(minVRuntime, peek().getVRuntime());
  }

  /** Removes a RUNNING entity temporarily from the runQueue. */
  void removeRunning(Entity entity) {
    if (!entity.isRunning()) {
      throw new IllegalStateException("entity should be RUNNING but is " + entity.getState());
    }
    if (!queue.contains(entity)) {
      throw new IllegalStateException("trying to update an entity that's not part of the runqueue");
    }

    queue.remove(entity);
  }

  /** Dequeues a no-longer-running entity from the runQueue. Updates the load accordingly. */
  void dequeue(Entity entity) {
    if (!entity.isDequeued()) {
      throw new IllegalStateException("entity should be DEQUEUED but is " + entity.getState());
    }
    if (!queue.contains(entity)) {
      throw new IllegalStateException("trying to update an entity that's not part of the runqueue");
    }

    queue.remove(entity);
    if (!entity.isRunningDequeued()) {
      leftMostWeight = queue.isEmpty() ? 0L : peek().getWeight();
      load -= entity.getWeight();
      assert load >= 0 : "Load cannot go negative";
    }
  }

  Iterator<Entity> iterator() {
    return queue.iterator();
  }
}
