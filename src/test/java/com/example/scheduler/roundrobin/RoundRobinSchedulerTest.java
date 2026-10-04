package com.example.scheduler.roundrobin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.example.scheduler.LoadBalancer;
import com.example.scheduler.Scheduler;
import com.example.scheduler.SimTask;
import com.example.scheduler.ThreadPoolExecutorSim;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class RoundRobinSchedulerTest {

  private static void runToCompletion(RoundRobinScheduler scheduler) {
    while (!scheduler.isEmpty()) {
      scheduler.runNext();
    }
  }

  @Test
  public void equalWorkTasksGetEqualNumberOfTurns() {
    RoundRobinScheduler scheduler = new RoundRobinScheduler(1);
    SimTask a = new SimTask("A", 10);
    SimTask b = new SimTask("B", 10);
    SimTask c = new SimTask("C", 10);
    scheduler.submit(a);
    scheduler.submit(b);
    scheduler.submit(c);

    runToCompletion(scheduler);

    assertEquals(10, a.getTimesScheduled());
    assertEquals(10, b.getTimesScheduled());
    assertEquals(10, c.getTimesScheduled());
  }

  @Test
  public void weightHasNoEffectOnSchedulingOrCpuShare() {
    // RR has no notion of priority: a "heavy" task (weight 10) and a "light" one (weight 1) with
    // the same amount of work get identical treatment and finish at the same time.
    RoundRobinScheduler scheduler = new RoundRobinScheduler(1);
    SimTask heavy = new SimTask("heavy", 20, 10);
    SimTask light = new SimTask("light", 20, 1);
    scheduler.submit(heavy);
    scheduler.submit(light);

    int steps = 0;
    while (!scheduler.isEmpty()) {
      scheduler.runNext();
      steps++;
    }

    assertEquals(40, steps);
    assertEquals(heavy.getConsumed(), light.getConsumed());
    assertEquals(20, heavy.getTimesScheduled());
    assertEquals(20, light.getTimesScheduled());
  }

  @Test
  public void tasksAreVisitedInFifoOrderEachRound() {
    RoundRobinScheduler scheduler = new RoundRobinScheduler(1);
    scheduler.submit(new SimTask("A", 100));
    scheduler.submit(new SimTask("B", 100));
    scheduler.submit(new SimTask("C", 100));

    for (int round = 0; round < 5; round++) {
      assertEquals("A", scheduler.runNext());
      assertEquals("B", scheduler.runNext());
      assertEquals("C", scheduler.runNext());
    }
  }

  // --- multi-thread load balancing ---

  @Test
  public void stealRemovesATaskWithoutRunningIt() {
    RoundRobinScheduler scheduler = new RoundRobinScheduler(1);
    scheduler.submit(new SimTask("A", 10));
    scheduler.submit(new SimTask("B", 10));

    SimTask stolen = scheduler.steal();

    assertEquals("B", stolen.getId()); // steal() takes from the tail, the opposite end of runNext()
    assertEquals(0, stolen.getConsumed()); // not run, just relocated
    assertEquals(1, scheduler.size());
  }

  @Test
  public void stealOnAnEmptyQueueReturnsNull() {
    RoundRobinScheduler scheduler = new RoundRobinScheduler(1);
    assertNull(scheduler.steal());
    assertEquals(0, scheduler.size());
  }

  @Test
  public void sizeTracksQueueLengthAsTasksFinish() {
    RoundRobinScheduler scheduler = new RoundRobinScheduler(5);
    scheduler.submit(new SimTask("A", 5));
    scheduler.submit(new SimTask("B", 5));
    assertEquals(2, scheduler.size());

    scheduler.runNext(); // A finishes in a single quantum of 5
    assertEquals(1, scheduler.size());

    scheduler.runNext(); // B finishes too
    assertEquals(0, scheduler.size());
  }

  @Test
  public void pickBusiestReturnsMinusOneWhenNobodyHasSpareWork() {
    Scheduler[] schedulers = {new RoundRobinScheduler(1), new RoundRobinScheduler(1)};
    schedulers[1].submit(new SimTask("only", 100)); // size 1, not > 1

    assertEquals(-1, LoadBalancer.pickBusiest(schedulers, 0));
  }

  @Test
  public void pickBusiestExcludesTheGivenIndexEvenIfItsTheBusiestOverall() {
    Scheduler[] schedulers = {new RoundRobinScheduler(1), new RoundRobinScheduler(1)};
    schedulers[0].submit(new SimTask("a", 100));
    schedulers[0].submit(new SimTask("b", 100));
    schedulers[0].submit(new SimTask("c", 100));

    // schedulers[0] is by far the busiest, but it's also the one being excluded (e.g. because
    // it's the one asking for work, not a candidate to steal from itself)
    assertEquals(-1, LoadBalancer.pickBusiest(schedulers, 0));
  }

  @Test
  public void loadBalancerStealsFromBusiestScheduler() {
    Scheduler[] schedulers = {
      new RoundRobinScheduler(1), new RoundRobinScheduler(1), new RoundRobinScheduler(1)
    };
    schedulers[1].submit(new SimTask("t1", 100));
    schedulers[1].submit(new SimTask("t2", 100));
    schedulers[1].submit(new SimTask("t3", 100));
    schedulers[2].submit(new SimTask("t4", 100)); // size 1 - not busy enough to be picked

    assertTrue(LoadBalancer.rebalance(schedulers, 0));

    assertEquals(1, schedulers[0].size());
    assertEquals(2, schedulers[1].size());
    assertEquals(1, schedulers[2].size());
  }

  @Test
  public void loadBalancerDoesNothingWhenNoSchedulerHasSpareWork() {
    Scheduler[] schedulers = {new RoundRobinScheduler(1), new RoundRobinScheduler(1)};
    schedulers[1].submit(new SimTask("only", 100));

    assertFalse(LoadBalancer.rebalance(schedulers, 0));
  }

  @Test
  public void multiThreadedExecutionRunsEveryTaskExactlyOnce() throws InterruptedException {
    int numThreads = 4;
    int numTasks = 40;
    SimTask[] tasks = new SimTask[numTasks];

    ThreadPoolExecutorSim pool = new ThreadPoolExecutorSim(numThreads, () -> new RoundRobinScheduler(1));
    pool.start();
    try {
      for (int i = 0; i < numTasks; i++) {
        tasks[i] = new SimTask("t" + i, 50);
        pool.submit(tasks[i]);
      }

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
        // exactly the task's total work, never more - proves no task ran twice under migration
        assertEquals(50, t.getConsumed());
      }
    } finally {
      pool.close();
    }
  }

  // --- real work: every tick runs actual code, not just a counter ---

  @Test
  public void everyTickRunsTheTasksRealCodeInTurnOrder() {
    // Each task appends its own id to a shared log on every tick. With a quantum of 2 the log
    // is the real execution trace: two ticks of A, two of B, and so on, round after round.
    List<String> log = new ArrayList<>();
    RoundRobinScheduler scheduler = new RoundRobinScheduler(2);
    scheduler.submit(new SimTask("A", 4, () -> log.add("A")));
    scheduler.submit(new SimTask("B", 4, () -> log.add("B")));
    scheduler.submit(new SimTask("C", 2, () -> log.add("C")));

    runToCompletion(scheduler);

    assertEquals(
        Arrays.asList("A", "A", "B", "B", "C", "C", "A", "A", "B", "B"), log);
  }

  @Test
  public void interleavedTasksStillComputeTheCorrectResults() {
    // Each task really computes 1 + 2 + ... + n, adding one number per tick. The scheduler
    // interleaves them arbitrarily, yet each sum must come out exactly right.
    int[] sizes = {10, 25, 100};
    long[] sums = new long[sizes.length];
    RoundRobinScheduler scheduler = new RoundRobinScheduler(3);
    for (int i = 0; i < sizes.length; i++) {
      int index = i;
      long[] next = {1};
      scheduler.submit(
          new SimTask("sum" + sizes[i], sizes[i], () -> sums[index] += next[0]++));
    }

    runToCompletion(scheduler);

    for (int i = 0; i < sizes.length; i++) {
      assertEquals(sizes[i] * (sizes[i] + 1L) / 2, sums[i]);
    }
  }

  @Test
  public void aTaskThatThrowsKeepsTheTicksItCompleted() {
    long[] runs = {0};
    SimTask task =
        new SimTask(
            "flaky",
            10,
            () -> {
              if (++runs[0] == 4) {
                throw new IllegalStateException("boom");
              }
            });

    try {
      task.runFor(10);
      throw new AssertionError("expected the task's exception to propagate");
    } catch (IllegalStateException expected) {
      assertEquals("boom", expected.getMessage());
    }

    assertEquals(3, task.getConsumed()); // the three ticks before the failure
    assertFalse(task.isDone());
  }

  @Test
  public void realTasksRunOnRealWorkerThreads() throws InterruptedException {
    int numThreads = 4;
    int numTasks = 20;
    long work = 200;
    Set<String> threadsSeen = ConcurrentHashMap.newKeySet();
    AtomicLong totalTicksExecuted = new AtomicLong();
    SimTask[] tasks = new SimTask[numTasks];

    ThreadPoolExecutorSim pool =
        new ThreadPoolExecutorSim(numThreads, () -> new RoundRobinScheduler(5));
    pool.start();
    try {
      for (int i = 0; i < numTasks; i++) {
        tasks[i] =
            new SimTask(
                "t" + i,
                work,
                () -> {
                  threadsSeen.add(Thread.currentThread().getName());
                  totalTicksExecuted.incrementAndGet();
                });
        pool.submit(tasks[i]);
      }

      long deadline = System.currentTimeMillis() + 5000;
      while (!allDone(tasks) && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }

      assertTrue("all tasks should finish within the deadline", allDone(tasks));
      // the real code ran exactly once per tick - never skipped, never duplicated by migration
      assertEquals(numTasks * work, totalTicksExecuted.get());
      // and it ran on the pool's worker threads, several of them, not on the test thread
      assertTrue("expected work on several workers, saw " + threadsSeen, threadsSeen.size() > 1);
      for (String name : threadsSeen) {
        assertTrue(name, name.startsWith("sim-worker-"));
      }
    } finally {
      pool.close();
    }
  }

  private static boolean allDone(SimTask[] tasks) {
    for (SimTask t : tasks) {
      if (!t.isDone()) {
        return false;
      }
    }
    return true;
  }
}
