package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;

import com.example.scheduler.SimTask;
import com.example.scheduler.TaskHandle;
import org.junit.Test;

/**
 * Direct unit tests for BasicStatsObserver: exercised here with hand-built TaskHandles rather than
 * a real TaskManager, since the observer only ever reacts to the events it's handed - no scheduling
 * logic of its own to drive.
 */
public class BasicStatsObserverTest {

  private static TaskHandle handleFor(SimTask task) {
    return new TaskHandle() {
      @Override
      public SimTask getTask() {
        return task;
      }

      @Override
      public void reEnqueue() {}

      @Override
      public int getThread() {
        return 0;
      }

      @Override
      public int getCurrentTaskLoad() {
        return 0;
      }
    };
  }

  @Test
  public void addTaskTracksTotalsRunnableAndPeaks() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 0);
    TaskHandle h1 = handleFor(new SimTask("a", 10));
    TaskHandle h2 = handleFor(new SimTask("b", 10));

    observer.addTask(h1, 0);
    observer.addTask(h2, 0);

    assertEquals(2, observer.getTotalTasks(0));
    assertEquals(2, observer.getRunnableTasks(0));
    assertEquals(2, observer.getPeakRunnableTasks(0));
    assertEquals(2, observer.getAllCurrentActiveTasks());
    assertEquals(2, observer.getAllPeakActiveTasks());
  }

  @Test
  public void peakRunnableTasksDoesNotDropWhenRunnableCountLaterFalls() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 0);
    SimTask a = new SimTask("a", 10);
    SimTask b = new SimTask("b", 10);
    TaskHandle ha = handleFor(a);
    TaskHandle hb = handleFor(b);

    observer.addTask(ha, 0);
    observer.addTask(hb, 0);
    assertEquals(2, observer.getPeakRunnableTasks(0));

    observer.dequeueTask(ha, 0); // a isn't done, just no longer runnable (e.g. blocked)
    assertEquals(1, observer.getRunnableTasks(0));
    assertEquals(2, observer.getPeakRunnableTasks(0)); // peak is a high-water mark
  }

  @Test
  public void dequeueOnlyDropsActiveCountWhenTheTaskIsActuallyDone() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 0);
    SimTask blocked = new SimTask("blocked", 10);
    SimTask finished = new SimTask("finished", 10);
    TaskHandle blockedHandle = handleFor(blocked);
    TaskHandle finishedHandle = handleFor(finished);

    observer.addTask(blockedHandle, 0);
    observer.addTask(finishedHandle, 0);
    assertEquals(2, observer.getAllCurrentActiveTasks());

    blocked.block();
    observer.dequeueTask(blockedHandle, 0);
    assertEquals(2, observer.getAllCurrentActiveTasks()); // still active - just blocked, not done

    finished.runFor(10); // drives it to DONE
    observer.dequeueTask(finishedHandle, 0);
    assertEquals(1, observer.getAllCurrentActiveTasks());
  }

  @Test
  public void switchTaskCountsAScheduleOnlyWhenThereIsANextTask() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 0);
    TaskHandle h = handleFor(new SimTask("a", 10));

    observer.switchTask(null, h, 0);
    assertEquals(1, observer.getNumSchedule(0));

    observer.switchTask(h, null, 0); // going idle - not a schedule
    assertEquals(1, observer.getNumSchedule(0));
  }

  @Test
  public void enqueueTaskIncrementsOnlyTheDestinationThread() {
    BasicStatsObserver observer = new BasicStatsObserver(2, 0);
    TaskHandle h = handleFor(new SimTask("a", 10));

    observer.enqueueTask(h, 0, 1);

    assertEquals(0, observer.getRunnableTasks(0));
    assertEquals(1, observer.getRunnableTasks(1));
  }

  @Test
  public void rebalanceMovesRunnableCountAndTracksMigrationsIntoDestination() {
    BasicStatsObserver observer = new BasicStatsObserver(2, 0);
    TaskHandle h = handleFor(new SimTask("a", 10));
    observer.addTask(h, 0);

    observer.rebalance(h, 0, 1);

    assertEquals(0, observer.getRunnableTasks(0));
    assertEquals(1, observer.getRunnableTasks(1));
    assertEquals(1, observer.getNumMigrationsIn(1));
    assertEquals(0, observer.getNumMigrationsIn(0));
  }

  @Test
  public void workRequestRejectedIsTrackedPerThread() {
    BasicStatsObserver observer = new BasicStatsObserver(3, 0);
    observer.workRequestRejected(2);
    observer.workRequestRejected(2);

    assertEquals(2, observer.getNumWorkRequestsRejected(2));
    assertEquals(0, observer.getNumWorkRequestsRejected(0));
  }

  @Test
  public void extensionPoolRejectionReasonsHaveIndependentCounters() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 1);

    observer.rejectedDueToCpuBusy();
    observer.rejectedDueToCpuBusy();
    observer.rejectedDueToHighLoadAverage();
    observer.rejectedDueToNoExtensionThreads();

    assertEquals(2, observer.getCpuBusyRejections());
    assertEquals(1, observer.getLoadAverageRejections());
    assertEquals(1, observer.getPoolFullRejections());
  }

  @Test
  public void addToExtensionThreadTracksRunsAndDropsActiveCountOnlyWhenDone() {
    BasicStatsObserver observer = new BasicStatsObserver(1, 1);
    TaskHandle h = handleFor(new SimTask("a", 10));
    observer.addTask(h, 0);
    assertEquals(1, observer.getAllCurrentActiveTasks());

    observer.addToExtensionThread(0, 3, false);
    assertEquals(3, observer.getExtensionNumRuns(0));
    assertEquals(1, observer.getExtensionTotalTasks(0));
    assertEquals(1, observer.getAllCurrentActiveTasks()); // not done yet

    observer.addToExtensionThread(0, 2, true);
    assertEquals(5, observer.getExtensionNumRuns(0));
    assertEquals(2, observer.getExtensionTotalTasks(0));
    assertEquals(0, observer.getAllCurrentActiveTasks());
  }
}
