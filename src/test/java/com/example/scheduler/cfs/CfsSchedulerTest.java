package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.example.scheduler.Observer;
import com.example.scheduler.SimTask;
import com.example.scheduler.TaskHandle;
import com.example.scheduler.WakeUpListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Exercises TaskManager and the Entity/TaskGroup hierarchy directly - most tests drive a thread's
 * ThreadScheduler.getTask by hand (see Driver) with a virtual tick counter, exactly like
 * SlicingThread would, but from one test thread so behavior is deterministic. Only
 * multiThreadedExecutionRunsEveryTaskExactlyOnce uses real background threads, to prove the actual
 * locking is correct under real concurrency.
 */
public class CfsSchedulerTest {

  private static final long HUGE_WORK = 10_000_000;
  private static final WakeUpListener NOOP = () -> {};

  /** Drives one thread's ThreadScheduler exactly like SlicingThread would. */
  private static final class Driver {
    private final ThreadScheduler scheduler;
    private long clock;
    private TaskHandle current;

    Driver(ThreadScheduler scheduler) {
      this.scheduler = scheduler;
      this.current = scheduler.getTask(clock);
    }

    /** Runs whatever's current for up to slice ticks, and returns the task id that ran. */
    String runOnce(long slice) {
      if (current == null) {
        current = scheduler.getTask(clock);
      }
      if (current == null) {
        return null;
      }
      String id = current.getTask().getId();
      clock += current.getTask().runFor(slice);
      current = scheduler.getTask(clock);
      return id;
    }

    /**
     * Lets the scheduler re-evaluate without running anything - e.g. to notice a task became
     * BLOCKED (by other, external test code) since the last runOnce.
     */
    void tick() {
      current = scheduler.getTask(clock);
    }
  }

  // --- core CFS fairness, single thread (WeightBasedTaskEntity is the new bit vs. before) ---

  @Test
  public void weightBasedGroupGivesStrictPriorityNotJustProportionalShare() {
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, /* weightBasedScheduler= */ true);

    SimTask high = new SimTask("high", 5, 10);
    SimTask low = new SimTask("low", 5, 1);
    group.addTask(low, 1); // submitted first ...
    group.addTask(high, 10); // ... but higher weight must still run first, every time

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 5; i++) {
      assertEquals("high", driver.runOnce(1));
    }
    assertEquals(0, low.getConsumed()); // low never got a single tick while high was runnable
    assertTrue(high.isDone());

    for (int i = 0; i < 5; i++) {
      assertEquals("low", driver.runOnce(1));
    }
    assertTrue(low.isDone());
  }

  @Test
  public void plainVruntimeGroupStillGivesProportionalShareNotStrictPriority() {
    // same setup as above, but NOT weight-based: proportional CFS share instead of strict order.
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, false);

    SimTask heavy = new SimTask("heavy", HUGE_WORK, 2);
    SimTask light = new SimTask("light", HUGE_WORK, 1);
    group.addTask(heavy, 2);
    group.addTask(light, 1);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 1000; i++) {
      driver.runOnce(1000);
    }

    double ratio = heavy.getConsumed() / (double) light.getConsumed();
    assertTrue("expected ~2x share, got " + ratio, ratio > 1.8 && ratio < 2.2);
  }

  @Test
  public void minVRuntimeStopsALateJoinerFromMonopolizingTheThread() {
    // if a new task started at vruntime=0 it would dominate for a very long time (everyone
    // else's vruntime has already advanced past 0); min_vruntime is what prevents that.
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, false);
    SimTask a = new SimTask("A", HUGE_WORK, 1);
    SimTask b = new SimTask("B", HUGE_WORK, 1);
    group.addTask(a, 1);
    group.addTask(b, 1);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 50; i++) {
      driver.runOnce(1);
    }

    SimTask c = new SimTask("C", HUGE_WORK, 1);
    group.addTask(c, 1);

    Map<String, Integer> counts = new HashMap<>();
    String previous = null;
    int currentStreak = 0;
    int maxStreak = 0;
    for (int i = 0; i < 30; i++) {
      String id = driver.runOnce(1);
      counts.merge(id, 1, Integer::sum);
      currentStreak = id.equals(previous) ? currentStreak + 1 : 1;
      maxStreak = Math.max(maxStreak, currentStreak);
      previous = id;
    }

    assertTrue("A got " + counts.getOrDefault("A", 0) + " turns", counts.getOrDefault("A", 0) >= 8);
    assertTrue("B got " + counts.getOrDefault("B", 0) + " turns", counts.getOrDefault("B", 0) >= 8);
    assertTrue("C got " + counts.getOrDefault("C", 0) + " turns", counts.getOrDefault("C", 0) >= 8);
    assertTrue("longest consecutive streak for one task was " + maxStreak, maxStreak <= 2);
  }

  @Test
  public void twoSingleTaskGroupsOfEqualWeightSplitCpuEvenly() {
    // the clean baseline for group-vs-group fairness: with exactly one task each, both groups'
    // entities leave the runqueue while running (see Entity#prepareToRun's empty-vs-nonempty
    // branch in GroupEntity) - symmetrically, so groupPenalty never gets a chance to engage (it
    // needs to see 2+ groups sitting in the queue *at once*, which never happens here) and this
    // reduces to plain, exact vruntime fairness at the group level.
    //
    // A *multi*-member group behaves differently (its entity stays in the queue, marked
    // RUNNING_ENQUEUED, whenever any of its other members are still runnable) - which is a real,
    // structural asymmetry in the state machine, not modeled by this simpler test. It
    // means "does grouping N tasks together get exactly the same total CPU as 1" isn't a clean
    // invariant of this algorithm once groupPenalty can engage; see
    // groupPenaltyReducesAGroupsLocalShareToCompensateForExtraCpuElsewhere for the (approximate,
    // documented) property that *does* hold across threads.
    TaskManager manager = new TaskManager(1);
    TaskGroup groupA = manager.newGroup(1, false);
    TaskGroup groupB = manager.newGroup(1, false);

    SimTask a = new SimTask("a", HUGE_WORK, 1);
    SimTask b = new SimTask("b", HUGE_WORK, 1);
    groupA.addTask(a, 1);
    groupB.addTask(b, 1);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 600; i++) {
      driver.runOnce(1000);
    }

    double ratio = a.getConsumed() / (double) b.getConsumed();
    assertTrue(
        "expected two equal-weight, single-task groups to split CPU ~50/50, got ratio=" + ratio,
        ratio > 0.9 && ratio < 1.1);
  }

  @Test
  public void weightBasedGroupSharesProportionallyAmongEqualWeightSiblings() {
    // WeightBasedTaskEntity#compareTo only enforces strict order *across* weight classes; within
    // the same weight, it falls back to ordinary vruntime comparison, so equal-weight siblings
    // still get plain CFS proportional sharing between themselves.
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, /* weightBasedScheduler= */ true);
    SimTask a = new SimTask("a", HUGE_WORK, 1);
    SimTask b = new SimTask("b", HUGE_WORK, 1);
    group.addTask(a, 1);
    group.addTask(b, 1);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 1000; i++) {
      driver.runOnce(1000);
    }

    double ratio = a.getConsumed() / (double) b.getConsumed();
    assertTrue(
        "expected ~50/50 share between equal-weight siblings, got " + ratio,
        ratio > 0.9 && ratio < 1.1);
  }

  // --- group scheduling: cross-thread fairness via groupPenalty ---

  @Test
  public void groupPenaltyReducesAGroupsLocalShareToCompensateForExtraCpuElsewhere() {
    // Group A has a task on BOTH threads; groups B and C only have a task on thread 0 each.
    // (updateShares() only ever sees the groups *currently sitting in thread 0's queue*, which
    // excludes whichever one is mid-run - with only 2 groups that's always exactly 1, i.e. never
    // enough to compare; 3 groups is the minimum for it to ever see 2 at once and engage at all.)
    //
    // Without groupPenalty, A/B/C would split thread 0 evenly (1/3 each), on top of which A
    // *also* gets all of thread 1 - a structural advantage no per-thread mechanism can erase,
    // since B and C were simply never placed on thread 1 to begin with. What groupPenalty *can*
    // do, and what this asserts, is reduce A's share of thread 0 below that naive 1/3, to at
    // least partially compensate for the extra thread A gets almost for free.
    TaskManager manager = new TaskManager(2);
    TaskGroup groupA = manager.newGroup(1, false);
    TaskGroup groupB = manager.newGroup(1, false);
    TaskGroup groupC = manager.newGroup(1, false);

    SimTask a0 = new SimTask("a0", HUGE_WORK, 1);
    SimTask a1 = new SimTask("a1", HUGE_WORK, 1);
    SimTask b0 = new SimTask("b0", HUGE_WORK, 1);
    SimTask c0 = new SimTask("c0", HUGE_WORK, 1);

    // bypass the load-balancer's automatic thread placement to pin tasks to specific threads,
    // reproducing the exact "uneven spread" scenario groupPenalty exists for.
    manager.addTaskEntity(groupA.newTaskEntity(0, a0, 1));
    manager.addTaskEntity(groupB.newTaskEntity(0, b0, 1));
    manager.addTaskEntity(groupC.newTaskEntity(0, c0, 1));
    manager.addTaskEntity(groupA.newTaskEntity(1, a1, 1));

    Driver thread0 = new Driver(manager.getThreadScheduler(0, NOOP));
    Driver thread1 = new Driver(manager.getThreadScheduler(1, NOOP));
    for (int i = 0; i < 2000; i++) {
      thread0.runOnce(1000);
      thread1.runOnce(1000);
    }

    long thread0Total = a0.getConsumed() + b0.getConsumed() + c0.getConsumed();
    double a0Share = a0.getConsumed() / (double) thread0Total;
    assertTrue(
        "expected A's share of thread 0 to be reduced well below the naive 1/3, got " + a0Share,
        a0Share < 0.25);
    // and B/C, which have no advantage over each other, should still split what's left evenly
    assertTrue(
        Math.abs(b0.getConsumed() - c0.getConsumed()) < 0.1 * Math.max(b0.getConsumed(), 1));
  }

  // --- blocking / re-enqueue ---

  @Test
  public void blockedTaskIsDequeuedAndReenqueuesOnceUnblocked() {
    TaskManager manager = new TaskManager(1, false, /* rescheduleOnUnblock= */ true, false);
    SimTask task = new SimTask("t", 100);
    TaskHandle handle = manager.addTask(task, 1);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    driver.runOnce(10);
    assertEquals(10, task.getConsumed());

    task.block();
    // next getTask() call notices it's no longer runnable and dequeues it
    driver.tick();
    assertEquals(0, manager.getNumRunnableTasks());
    assertEquals(10, task.getConsumed()); // didn't run again while blocked

    task.unblock();
    handle.reEnqueue(); // staged, but not yet drained into the runqueue
    driver.tick(); // next getTask() call drains staging via handleMessages()
    assertEquals(1, manager.getNumRunnableTasks());

    driver.runOnce(10);
    assertEquals(20, task.getConsumed());
  }

  // --- load balancing ---

  @Test
  public void idleThreadRequestsAndReceivesWorkFromBusiestThread() {
    // onIdleLoadShed is what actually lets a busy thread proactively hand over a task when it
    // notices a pending request, rather than only reacting once it goes idle itself.
    TaskManager manager = new TaskManager(2, /* onIdleLoadShed= */ true, false, false);
    TaskGroup group = manager.newGroup(1, false);
    SimTask busy1 = new SimTask("busy1", HUGE_WORK, 1);
    SimTask busy2 = new SimTask("busy2", HUGE_WORK, 1);
    SimTask busy3 = new SimTask("busy3", HUGE_WORK, 1);

    // pin all 3 tasks onto thread 0, leave thread 1 with nothing
    manager.addTaskEntity(group.newTaskEntity(0, busy1, 1));
    manager.addTaskEntity(group.newTaskEntity(0, busy2, 1));
    manager.addTaskEntity(group.newTaskEntity(0, busy3, 1));

    Driver thread0 = new Driver(manager.getThreadScheduler(0, NOOP));
    // constructing thread 1's driver already finds nothing and requests work from thread 0
    Driver thread1 = new Driver(manager.getThreadScheduler(1, NOOP));
    assertNull(thread1.runOnce(1)); // still nothing - the request hasn't been serviced yet

    // thread 0's next call notices the pending request and sheds a task to satisfy it
    thread0.runOnce(1);

    // thread 1 now finds the migrated task waiting
    assertNotNull(thread1.runOnce(1));
    assertEquals(3, manager.getNumRunnableTasks()); // nothing lost or duplicated in the shuffle
  }

  // --- RPC-offload (OffloadingTaskManager / DeferredTaskQueue / ReEnqueueThread) ---

  @Test
  public void offloadingTaskManagerBatchesReenqueueOntoDedicatedThread() throws InterruptedException {
    OffloadingTaskManager manager =
        new OffloadingTaskManager(1, 0, Observer.DUMMY, false, true, false, null, null, false);
    try {
      SimTask task = new SimTask("t", 100);
      TaskHandle handle = manager.addTask(task, 1);

      Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
      driver.runOnce(10);
      task.block();
      driver.tick();
      assertEquals(0, manager.getNumRunnableTasks());

      task.unblock();
      handle.reEnqueue(); // offloaded: goes to the DeferredTaskQueue, NOT enqueued yet
      assertEquals(0, manager.getNumRunnableTasks());

      manager.processBulkEnqueue(1000); // the ReEnqueueThread's loop body, run synchronously here
      driver.tick(); // next getTask() call drains staging via handleMessages()
      assertEquals(1, manager.getNumRunnableTasks());

      driver.runOnce(10);
      assertEquals(20, task.getConsumed());
    } finally {
      manager.close();
    }
  }

  // --- extension pool (RunQueueExtender / ExtensionMQ) ---

  @Test
  public void extensionPoolBorrowsAThreadForAnUnblockedTaskWhenBaseThreadsAreBusy() {
    double[] cpuLoad = {0.1}; // well under the 0.8 threshold
    double[] loadAverage = {0.0}; // well under the threshold too
    TaskManager manager =
        new TaskManager(
            1,
            1,
            Observer.DUMMY,
            false,
            /* rescheduleOnUnblock= */ true,
            false,
            () -> cpuLoad[0],
            () -> loadAverage[0],
            /* extensionPoolEnabled= */ true);

    TaskGroup group = manager.newGroup(1, /* weightBasedScheduler= */ true);
    SimTask filler = new SimTask("filler", HUGE_WORK, 1);
    SimTask extensible = new SimTask("extensible", 100, 2, /* eligibleForExtensionPool= */ true);
    group.addTask(filler, 1);
    // higher weight so it's picked (and thus "current") first, deterministically, in this
    // weight-based (strict-priority) group
    TaskHandle handle = group.addTask(extensible, 2);

    // an extension-pool worker thread would normally call this once on startup to register itself
    ThreadScheduler extensionThread = manager.getThreadScheduler(1, NOOP);

    Driver base = new Driver(manager.getThreadScheduler(0, NOOP));
    assertEquals("extensible", base.runOnce(1)); // higher weight -> current first

    extensible.block();
    base.tick(); // notices extensible is blocked, dequeues it; filler is still there (load=1)

    extensible.unblock();
    handle.reEnqueue(); // base thread looks loaded -> routed to the extension pool instead

    Driver ext = new Driver(extensionThread);
    assertEquals("extensible", ext.runOnce(10));
    assertEquals(11, extensible.getConsumed()); // 1 tick before it blocked + 10 on the extension thread
  }

  @Test
  public void extensionPoolRejectsWhenCpuIsBusy() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 1);
    TaskManager manager =
        new TaskManager(
            1, 1, observer, false, /* rescheduleOnUnblock= */ true, false,
            () -> 0.9, /* cpu load above the 0.8 threshold */
            () -> 0.0,
            /* extensionPoolEnabled= */ true);
    TaskGroup group = manager.newGroup(1, /* weightBasedScheduler= */ true);
    SimTask filler = new SimTask("filler", HUGE_WORK, 1);
    SimTask extensible = new SimTask("extensible", 100, 2, /* eligibleForExtensionPool= */ true);
    group.addTask(filler, 1);
    TaskHandle handle = group.addTask(extensible, 2);
    manager.getThreadScheduler(1, NOOP); // registers the (otherwise unused) extension thread

    Driver base = new Driver(manager.getThreadScheduler(0, NOOP));
    base.runOnce(1); // extensible current (higher weight)
    extensible.block();
    base.tick();
    extensible.unblock();
    handle.reEnqueue(); // would qualify, but the CPU looks too busy to borrow a thread

    assertEquals(1, observer.getCpuBusyRejections());
    assertEquals(0, observer.getPoolFullRejections());
  }

  @Test
  public void extensionPoolRejectsWhenNoExtensionThreadsAreFree() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 1);
    TaskManager manager =
        new TaskManager(
            1, 1, observer, false, /* rescheduleOnUnblock= */ true, false,
            () -> 0.1, () -> 0.0, /* extensionPoolEnabled= */ true);
    TaskGroup group = manager.newGroup(1, /* weightBasedScheduler= */ true);
    SimTask filler = new SimTask("filler", HUGE_WORK, 1);
    SimTask ext1 = new SimTask("ext1", HUGE_WORK, 5, /* eligibleForExtensionPool= */ true);
    SimTask ext2 = new SimTask("ext2", 100, 4, /* eligibleForExtensionPool= */ true);
    group.addTask(filler, 1);
    TaskHandle handle1 = group.addTask(ext1, 5);
    TaskHandle handle2 = group.addTask(ext2, 4);
    manager.getThreadScheduler(1, NOOP); // registers + frees the sole extension thread

    Driver base = new Driver(manager.getThreadScheduler(0, NOOP));
    assertEquals("ext1", base.runOnce(1)); // highest weight runs first
    ext1.block();
    base.tick();
    ext1.unblock();
    handle1.reEnqueue(); // takes the only free extension thread

    assertEquals("ext2", base.runOnce(1)); // next-highest weight now current
    ext2.block();
    base.tick();
    ext2.unblock();
    handle2.reEnqueue(); // qualifies, but the sole extension thread is already taken by ext1

    assertEquals(1, observer.getPoolFullRejections());
    assertEquals(0, observer.getCpuBusyRejections());
  }

  // --- real concurrency smoke test ---

  @Test
  public void multiThreadedExecutionRunsEveryTaskExactlyOnce() throws InterruptedException {
    int numThreads = 4;
    int numTasks = 40;
    TaskManager manager = new TaskManager(numThreads, false, true, false);
    SlicingThread[] threads = new SlicingThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new SlicingThread(i, manager, 10);
    }
    SimTask[] tasks = new SimTask[numTasks];
    for (int i = 0; i < numTasks; i++) {
      tasks[i] = new SimTask("t" + i, 50);
      manager.addTask(tasks[i], 1);
    }
    for (SlicingThread t : threads) {
      t.start();
    }
    try {
      long deadline = System.currentTimeMillis() + 5000;
      boolean allDone;
      do {
        allDone = true;
        for (SimTask t : tasks) {
          if (!t.isDone()) {
            allDone = false;
            break;
          }
        }
        if (!allDone) {
          Thread.sleep(10);
        }
      } while (!allDone && System.currentTimeMillis() < deadline);

      assertTrue("all tasks should finish within the deadline", allDone);
      for (SimTask t : tasks) {
        assertEquals(50, t.getConsumed());
      }
    } finally {
      for (SlicingThread t : threads) {
        t.close();
      }
    }
  }

  // --- real work: every tick runs actual code under CFS too ---

  @Test
  public void weightedTasksDoProportionallyMoreRealWork() {
    // Both tasks really increment their own counter on every tick. Under CFS the weight-2 task
    // is given about twice the ticks, so its counter ends up about twice as high.
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, false);
    long[] heavyCount = {0};
    long[] lightCount = {0};
    SimTask heavy = new SimTask("heavy", HUGE_WORK, 2, () -> heavyCount[0]++);
    SimTask light = new SimTask("light", HUGE_WORK, 1, () -> lightCount[0]++);
    group.addTask(heavy, 2);
    group.addTask(light, 1);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 1000; i++) {
      driver.runOnce(100);
    }

    assertEquals(heavy.getConsumed(), heavyCount[0]); // one real execution per tick
    assertEquals(light.getConsumed(), lightCount[0]);
    double ratio = heavyCount[0] / (double) lightCount[0];
    assertTrue("expected ~2x real work, got " + ratio, ratio > 1.8 && ratio < 2.2);
  }

  @Test
  public void strictPriorityRunsTheHighWeightTasksRealCodeFirst() {
    // The execution trace written by the tasks themselves shows the strict-priority order:
    // every tick of "high" happens before the first tick of "low".
    TaskManager manager = new TaskManager(1);
    TaskGroup group = manager.newGroup(1, true);
    java.util.List<String> trace = new java.util.ArrayList<>();
    SimTask high = new SimTask("high", 3, 10, () -> trace.add("high"));
    SimTask low = new SimTask("low", 3, 1, () -> trace.add("low"));
    group.addTask(low, 1);
    group.addTask(high, 10);

    Driver driver = new Driver(manager.getThreadScheduler(0, NOOP));
    for (int i = 0; i < 6; i++) {
      driver.runOnce(1);
    }

    assertEquals(
        java.util.Arrays.asList("high", "high", "high", "low", "low", "low"), trace);
  }

  @Test
  public void realTasksRunOnRealSlicingThreads() throws InterruptedException {
    int numThreads = 4;
    int numTasks = 40;
    long work = 50;
    TaskManager manager = new TaskManager(numThreads, false, true, false);
    SlicingThread[] threads = new SlicingThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new SlicingThread(i, manager, 10);
    }
    Set<String> threadsSeen = ConcurrentHashMap.newKeySet();
    AtomicLong totalTicksExecuted = new AtomicLong();
    SimTask[] tasks = new SimTask[numTasks];
    for (int i = 0; i < numTasks; i++) {
      tasks[i] =
          new SimTask(
              "t" + i,
              work,
              () -> {
                threadsSeen.add(Thread.currentThread().getName());
                totalTicksExecuted.incrementAndGet();
              });
      manager.addTask(tasks[i], 1);
    }
    for (SlicingThread t : threads) {
      t.start();
    }
    try {
      long deadline = System.currentTimeMillis() + 5000;
      boolean allDone;
      do {
        allDone = true;
        for (SimTask t : tasks) {
          if (!t.isDone()) {
            allDone = false;
            break;
          }
        }
        if (!allDone) {
          Thread.sleep(10);
        }
      } while (!allDone && System.currentTimeMillis() < deadline);

      assertTrue("all tasks should finish within the deadline", allDone);
      assertEquals(numTasks * work, totalTicksExecuted.get());
      assertTrue("expected work on several threads, saw " + threadsSeen, threadsSeen.size() > 1);
      for (String name : threadsSeen) {
        assertTrue(name, name.startsWith("slicing-thread-"));
      }
    } finally {
      for (SlicingThread t : threads) {
        t.close();
      }
    }
  }
}
