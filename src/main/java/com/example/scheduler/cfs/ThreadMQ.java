package com.example.scheduler.cfs;

import com.example.scheduler.TaskHandle;
import com.example.scheduler.WakeUpListener;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Message queue instance that handles communication between the per-thread schedulers, so each
 * scheduler stays unaware of the others. isActive() always returns true - this project doesn't
 * implement dynamic thread activation/deactivation.
 */
class ThreadMQ implements ThreadScheduler {

  private final int thread;
  private TaskProvider scheduler;

  /**
   * keep track of staging and work requests. Multiple threads can write to these queues but only
   * the scheduler thread can read from them.
   */
  private final DiffQueue<TaskEntity> staging = new DiffQueue<>();

  private final DiffQueue<Integer> requests = new DiffQueue<>();

  private final TaskManager manager;
  private final GroupEntity groupEntity;

  private WakeUpListener wakeUpListener;
  private final boolean onIdleLoadShed;

  private final AtomicInteger numTasks = new AtomicInteger(0);
  private TaskEntity current;

  ThreadMQ(TaskManager manager, GroupEntity groupEntity, WakeUpListener listener, boolean onIdleLoadShed) {
    this.manager = manager;
    this.groupEntity = groupEntity;
    this.scheduler = new DefaultTaskProvider(groupEntity.getRunQueue());
    if (listener == null) {
      throw new NullPointerException("wakeUp listener shouldn't be null");
    }
    this.wakeUpListener = listener;
    this.onIdleLoadShed = onIdleLoadShed;
    this.thread = groupEntity.getThread();
  }

  protected TaskEntity getCurrent() {
    return current;
  }

  /** Swaps in the real listener once the owning executor thread exists. */
  void setWakeUpListener(WakeUpListener listener) {
    if (listener == null) {
      throw new NullPointerException("wakeUp listener shouldn't be null");
    }
    this.wakeUpListener = listener;
  }

  /**
   * Called by the owning executor thread with the elapsed virtual time since its previous call, to
   * account for the just-finished task and pick the next one.
   *
   * @return handle of the most eligible task to run, null if none exists
   */
  @Override
  public TaskHandle getTask(long time) {
    return getTaskThreadActive(time);
  }

  private TaskHandle getTaskThreadActive(long time) {
    handleMessages();

    groupEntity.updateShares();

    if (current != null && !current.isRunnable()) {
      // current is no longer runnable, it will be dequeued from the runqueue
      manager.dequeueTask(current, thread);
      numTasks.decrementAndGet();
    }

    scheduler.taskDone(time);

    if (requests.size() > 0) {
      // the thread might be able to shed some tasks, leaving at least one in the runqueue
      migrateTasks();
    }

    current = scheduler.getTask();

    if (current == null) {
      // this thread is going idle, request more work from the load balancer
      manager.requestMoreWork(thread);
    } else {
      current.setCurrentLoad((int) getLoad());
    }
    // any requests that remain in the incoming queue could not be satisfied, and need to be
    // rejected. (A request that arrives an instant later than our migration attempt would be
    // rejected here; it'll turn around and ask another thread instead - an acceptable rare
    // one-tick delay, in exchange for much simpler code.)
    rejectRemainingMigrationRequests();

    return current;
  }

  int getNumTasks() {
    return numTasks.get();
  }

  int getNumStaged() {
    return staging.size();
  }

  int getNumWorkRequests() {
    return requests.size();
  }

  boolean isActive() {
    return true;
  }

  /** multiple threads can call this method */
  void add(TaskEntity st) {
    assert st.getThread() == thread;
    staging.add(st);
    wakeUpListener.wakeUpIfIdle();
  }

  /** multiple threads can call this method */
  void requestWork(int requestingThread) {
    assert this.thread != requestingThread : "scheduler should not receive a request from itself";
    requests.add(requestingThread);
  }

  /** multiple threads can call this method */
  long getLoad() {
    return numTasks.get() + getStagedLoad();
  }

  /** multiple threads can call this method */
  long getStagedLoad() {
    // take into account work requests we already received
    return staging.size() - requests.size();
  }

  int getThread() {
    return thread;
  }

  protected boolean hasWork() {
    boolean currentNoLongerRunnable = (current != null && !current.isRunnable());
    int work = numTasks.get() - (currentNoLongerRunnable ? 1 : 0);
    return work > 0;
  }

  protected void flushStaging() {
    // noop as thread MQ allows only same thread to flush from staging
  }

  protected void handleMessages() {
    // get current account of num requests/staged - more messages can still come in but they'll
    // be left for the next schedule
    int numR = requests.size();
    int numS = staging.size();

    if (Math.min(numR, numS) > 0 && !hasWork()) {
      // ensure we have at least one task to run
      TaskEntity taskEntity = staging.poll();
      if (taskEntity != null) {
        taskEntity.addToRunQueue();
        numTasks.incrementAndGet();
      }
      numS--;
    }

    while (numS > 0) {
      if (numR > 0) {
        // fulfill work request
        manager.acceptWorkMigration(staging.poll(), thread, requests.poll());
        numR--;
        numS--;
      } else {
        // add task to runQueue
        TaskEntity taskEntity = staging.poll();
        if (taskEntity != null) {
          taskEntity.addToRunQueue();
          numTasks.incrementAndGet();
        }
        numS--;
      }
    }
  }

  private void migrateTasks() {
    if (onIdleLoadShed) {
      int numR = requests.size();
      while (numR > 0 && numTasks.get() > 1) {
        final TaskEntity migTask = scheduler.getTaskToMigrate();
        if (migTask == null) {
          break;
        }
        manager.acceptWorkMigration(migTask, thread, requests.poll());
        numR--;
        numTasks.decrementAndGet();
      }
    }
  }

  protected void rejectRemainingMigrationRequestsHandler() {
    int numR = requests.size();
    while (numR > 0) {
      manager.rejectTaskMigration(requests.poll());
      numR--;
    }
  }

  /** reject all remaining incoming migration requests */
  void rejectRemainingMigrationRequests() {
    rejectRemainingMigrationRequestsHandler();
  }

  void wakeUp() {
    wakeUpListener.wakeUpIfIdle();
  }

  protected TaskProvider getScheduler() {
    return scheduler;
  }

  protected DiffQueue<TaskEntity> getStaging() {
    return staging;
  }

  protected DiffQueue<Integer> getRequests() {
    return requests;
  }

  protected TaskManager getManager() {
    return manager;
  }

  protected AtomicInteger getNumTaskCounter() {
    return numTasks;
  }

  protected void setScheduler(TaskProvider scheduler) {
    this.scheduler = scheduler;
  }

  GroupEntity getGroupEntity() {
    return groupEntity;
  }

  long getRelativeWeight(GroupEntity thisGroup, long thisWeight) {
    return thisGroup.getLeftMostWeight() - thisWeight;
  }
}
