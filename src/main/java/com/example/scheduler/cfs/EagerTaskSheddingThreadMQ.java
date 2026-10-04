package com.example.scheduler.cfs;

import com.example.scheduler.WakeUpListener;

/**
 * Instead of waiting to be asked, proactively hands work over as soon as a request comes in
 * (checked from handleMessages(), called on every getTask()), rather than only when this thread
 * itself goes idle. Needs ThreadSafeTaskProvider since requestWork can now be called by another
 * thread concurrently with this thread's own getTask()/taskDone().
 */
class EagerTaskSheddingThreadMQ extends ThreadMQ {

  EagerTaskSheddingThreadMQ(
      TaskManager manager, GroupEntity groupEntity, WakeUpListener listener, boolean onIdleLoadShed) {
    super(manager, groupEntity, listener, onIdleLoadShed);
    // override with a thread safe scheduler
    setScheduler(new ThreadSafeTaskProvider(groupEntity.getRunQueue()));
  }

  // multiple threads can call this method
  @Override
  void requestWork(int requestingThread) {
    if (hasWork()) {
      // cannot satisfy reject request - no early return: a task may still become migratable via
      // staging below
      getManager().rejectTaskMigration(requestingThread);
    }

    assert getThread() != requestingThread : "scheduler should not receive a request from itself";
    TaskEntity task = getStaging().poll();
    if (task != null) {
      getManager().acceptWorkMigration(task, getThread(), requestingThread);
      return;
    }

    task = getScheduler().getTaskToMigrate();
    if (task != null) {
      getManager().acceptWorkMigration(task, getThread(), requestingThread);
      getNumTaskCounter().decrementAndGet();
      return;
    }
    getManager().rejectTaskMigration(requestingThread);
  }

  @Override
  protected void handleMessages() {
    while (getStaging().size() > 0) {
      TaskEntity taskEntity = getStaging().poll();
      if (taskEntity != null) {
        synchronized (getScheduler()) {
          taskEntity.addToRunQueue();
        }
        getNumTaskCounter().incrementAndGet();
      }
    }
  }
}
