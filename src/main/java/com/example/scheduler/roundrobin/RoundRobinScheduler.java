package com.example.scheduler.roundrobin;

import com.example.scheduler.Scheduler;
import com.example.scheduler.SimTask;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Naive round-robin scheduler: tasks sit in a FIFO queue, and each gets exactly one fixed-size
 * quantum per turn before going to the back of the queue.
 *
 * Deliberately dumb: it has no notion of priority/weight at all, so it cannot express "this task
 * should get more CPU than that one". Fairness only exists in terms of turns taken, not CPU time
 * consumed - see the cfs package for the alternative.
 */
public final class RoundRobinScheduler implements Scheduler {

  private final Deque<SimTask> queue = new ArrayDeque<>();
  private final long quantum;

  public RoundRobinScheduler(long quantum) {
    if (quantum <= 0) {
      throw new IllegalArgumentException("quantum must be > 0");
    }
    this.quantum = quantum;
  }

  @Override
  public void submit(SimTask task) {
    queue.addLast(task);
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public String runNext() {
    SimTask task = queue.pollFirst();
    if (task == null) {
      return null;
    }
    task.runFor(quantum);
    if (!task.isDone()) {
      queue.addLast(task);
    }
    return task.getId();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public SimTask steal() {
    // take from the tail: under strict FIFO that's the task furthest from its next turn, so
    // moving it elsewhere is the least disruptive choice.
    return queue.pollLast();
  }
}
