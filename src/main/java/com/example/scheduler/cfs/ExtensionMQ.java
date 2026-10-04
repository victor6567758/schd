package com.example.scheduler.cfs;

import com.example.scheduler.SimTask;
import com.example.scheduler.Task;
import com.example.scheduler.TaskHandle;
import com.example.scheduler.WakeUpListener;

/**
 * A per-thread scheduler front for extension-pool threads. Only ever holds one task at a time
 * and has no runqueue of its own - RunQueueExtender decides when to hand it a task and when to
 * reclaim the thread.
 */
final class ExtensionMQ implements ThreadScheduler {

  private final WakeUpListener wakeUpListener;
  private final RunQueueExtender extensionManager;
  private final int threadNum;
  private TaskEntity current;
  private int iteration = 0;

  ExtensionMQ(int threadNum, RunQueueExtender extender, WakeUpListener listener) {
    this.threadNum = threadNum;
    this.wakeUpListener = listener;
    this.extensionManager = extender;
  }

  @Override
  public TaskHandle getTask(long time) {
    TaskHandle result = null;
    if (current != null) {
      if (current.isRunnable()) {
        if (iteration == 0) {
          result = toTaskHandle(current);
          iteration++;
        } else if (!extensionManager.handOverTaskEntityIfBusy(current)) {
          result = toTaskHandle(current);
          iteration++;
        } else {
          removeCurrentTask();
        }
      } else {
        removeCurrentTask();
      }
    }
    return result;
  }

  private TaskHandle toTaskHandle(TaskEntity entity) {
    return new TaskHandle() {
      @Override
      public SimTask getTask() {
        return entity.getTask();
      }

      @Override
      public void reEnqueue() {
        entity.reEnqueue();
      }

      @Override
      public int getThread() {
        return threadNum;
      }

      @Override
      public int getCurrentTaskLoad() {
        return 1;
      }
    };
  }

  private void removeCurrentTask() {
    boolean done = current.getTask().getState() == Task.State.DONE;
    int numRuns = iteration;
    iteration = 0;
    current = null;
    extensionManager.releaseThread(threadNum, numRuns, done);
  }

  void add(TaskEntity entity) {
    assert current == null;
    this.current = entity;
    this.wakeUpListener.wakeUpIfIdle();
  }
}
