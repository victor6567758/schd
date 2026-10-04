package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;

import com.example.scheduler.SimTask;
import com.example.scheduler.TaskHandle;
import com.example.scheduler.WakeUpListener;
import org.junit.Test;

/**
 * Traces a TaskEntity through the exact state machine documented in Entity's
 * javadoc: DEQUEUED (just submitted, still staged) -> ENQUEUED (drained into the runqueue) ->
 * RUNNING_DEQUEUED (picked to run - temporarily pulled out of the runqueue) -> back to ENQUEUED
 * or RUNNING_DEQUEUED again (still runnable) -> DEQUEUED (blocked or done).
 */
public class EntityStateMachineTest {

  private static final WakeUpListener NOOP = () -> {};

  @Test
  public void taskEntityCyclesThroughDequeuedEnqueuedAndRunningDequeued() {
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, false);
    SimTask task = new SimTask("t", 100);
    TaskHandle handle = group.addTask(task, 1);
    TaskEntity entity = (TaskEntity) handle;

    // just submitted: staged in the ThreadMQ, not yet touched by any runqueue
    assertEquals(Entity.State.DEQUEUED, entity.getState());

    ThreadScheduler ts = manager.getThreadScheduler(0, NOOP);
    ts.getTask(0); // handleMessages(): DEQUEUED -> ENQUEUED; then picked: ENQUEUED -> RUNNING_DEQUEUED

    assertEquals(Entity.State.RUNNING_DEQUEUED, entity.getState());

    task.runFor(10);
    ts.getTask(10); // taskDone()/prepareToStop(): RUNNING_DEQUEUED -> ENQUEUED -> picked again (only
    // task around) -> RUNNING_DEQUEUED

    assertEquals(Entity.State.RUNNING_DEQUEUED, entity.getState());
  }

  @Test
  public void blockingTheRunningTaskDequeuesItsEntityEntirely() {
    TaskManager manager = new TaskManager(1);
    SimTask task = new SimTask("t", 100);
    TaskHandle handle = manager.addTask(task, 1);
    TaskEntity entity = (TaskEntity) handle;

    ThreadScheduler ts = manager.getThreadScheduler(0, NOOP);
    ts.getTask(0); // ENQUEUED -> RUNNING_DEQUEUED
    task.runFor(10);
    task.block();

    ts.getTask(10); // notices it's no longer runnable: prepareToStop() still runs (vruntime
    // accounted for fairly) then removeFromRunQueue(): ENQUEUED -> DEQUEUED

    assertEquals(Entity.State.DEQUEUED, entity.getState());
  }

  @Test
  public void reenqueuingAfterUnblockGoesBackToEnqueuedThenRunningDequeued() {
    TaskManager manager = new TaskManager(1, false, /* rescheduleOnUnblock= */ true, false);
    SimTask task = new SimTask("t", 100);
    TaskHandle handle = manager.addTask(task, 1);
    TaskEntity entity = (TaskEntity) handle;

    ThreadScheduler ts = manager.getThreadScheduler(0, NOOP);
    ts.getTask(0);
    task.runFor(10);
    task.block();
    ts.getTask(10);
    assertEquals(Entity.State.DEQUEUED, entity.getState());

    task.unblock();
    handle.reEnqueue(); // staged again, but state doesn't flip until the next getTask() call
    assertEquals(Entity.State.DEQUEUED, entity.getState());

    ts.getTask(10); // handleMessages() drains it: DEQUEUED -> ENQUEUED -> picked: RUNNING_DEQUEUED
    assertEquals(Entity.State.RUNNING_DEQUEUED, entity.getState());
  }
}
