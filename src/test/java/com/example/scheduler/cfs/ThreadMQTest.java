package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.example.scheduler.SimTask;
import com.example.scheduler.WakeUpListener;
import org.junit.Test;

/**
 * Direct tests of ThreadMQ's staging/request-queue mechanics - the part of the CFS port that's
 * easiest to gloss over: a task handed to a thread doesn't appear in its runqueue immediately,
 * and a work request doesn't get serviced immediately either. Both only take effect the next time
 * that thread's ThreadMQ.getTask runs, via ThreadMQ.handleMessages. This is what lets
 * TaskManager.addTaskEntity and TaskManager.acceptWorkMigration be called from *any* thread without
 * needing to touch another thread's runqueue directly.
 */
public class ThreadMQTest {

  private static final WakeUpListener NOOP = () -> {};

  @Test
  public void aNewlyAddedTaskIsStagedUntilTheNextGetTaskCallDrainsIt() {
    TaskManager manager = new TaskManager(1);
    ThreadMQ mq = (ThreadMQ) manager.getThreadScheduler(0, NOOP);

    manager.addTask(new SimTask("t", 100), 1);

    // staged, not yet in the runqueue - handleMessages() hasn't run yet
    assertEquals(1, mq.getNumStaged());
    assertEquals(0, mq.getNumTasks());

    mq.getTask(0); // drains staging via handleMessages()

    assertEquals(0, mq.getNumStaged());
    assertEquals(1, mq.getNumTasks());
  }

  @Test
  public void loadCountsBothRunningAndStagedTasks() {
    TaskManager manager = new TaskManager(1);
    ThreadMQ mq = (ThreadMQ) manager.getThreadScheduler(0, NOOP);

    manager.addTask(new SimTask("a", 100), 1);
    manager.addTask(new SimTask("b", 100), 1);
    assertEquals(2, mq.getLoad()); // both still staged: numTasks=0, staged=2

    mq.getTask(0); // drains both into the runqueue

    assertEquals(2, mq.getNumTasks());
    assertEquals(0, mq.getNumStaged());
    assertEquals(2, mq.getLoad());
  }

  @Test
  public void aRequestThatCannotBeSatisfiedIsRejectedOnTheNextGetTaskCall() {
    TaskManager manager = new TaskManager(2); // rescheduleOnUnblock/onIdleLoadShed both default off
    ThreadMQ mq0 = (ThreadMQ) manager.getThreadScheduler(0, NOOP); // empty - nothing to give away
    boolean[] wokenUp = {false};
    manager.getThreadScheduler(1, () -> wokenUp[0] = true);

    mq0.requestWork(1); // "thread 1 is asking thread 0 for work"
    assertEquals(1, mq0.getNumWorkRequests());

    mq0.getTask(0); // nothing staged to hand over -> the request goes unfulfilled

    assertEquals(0, mq0.getNumWorkRequests()); // rejected, not left pending forever
    assertTrue("the requester should be woken up so it can try elsewhere", wokenUp[0]);
  }

  @Test
  public void onIdleLoadShedProactivelyShedsATaskWhenARequestArrivesMidRun() {
    // onIdleLoadShed is what lets a *busy* thread hand over work the moment it notices a pending
    // request, instead of only reacting once it goes idle itself (see requestMoreWork's
    // "max <= 1" check for the idle-reacting path, tested at the TaskManager level elsewhere).
    TaskManager manager = new TaskManager(2, /* onIdleLoadShed= */ true, false, false);
    ThreadMQ mq0 = (ThreadMQ) manager.getThreadScheduler(0, NOOP);
    manager.getThreadScheduler(1, NOOP);
    TaskGroup group = manager.newGroup(1, false);

    // pin both onto thread 0 directly - manager.addTask() would auto-balance the second one onto
    // thread 1 instead, since thread 0 already looks busier after staging the first
    manager.addTaskEntity(group.newTaskEntity(0, new SimTask("a", 10_000_000), 1));
    manager.addTaskEntity(group.newTaskEntity(0, new SimTask("b", 10_000_000), 1));
    mq0.getTask(0); // drains staging: both tasks now in the runqueue, one picked as current

    mq0.requestWork(1);
    assertEquals(1, mq0.getNumWorkRequests());
    assertEquals(2, mq0.getNumTasks());

    mq0.getTask(0); // notices the pending request and (since >1 task remains) sheds one

    assertEquals(0, mq0.getNumWorkRequests());
    assertEquals(1, mq0.getNumTasks());
  }
}
