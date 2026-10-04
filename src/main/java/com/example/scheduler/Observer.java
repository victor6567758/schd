package com.example.scheduler;

/**
 * Scheduling-event hooks used to collect stats. TaskManager calls these; BasicStatsObserver is
 * the real implementation, DUMMY is a no-op default.
 */
public interface Observer {

  Observer DUMMY =
      new Observer() {
        @Override
        public void addTask(TaskHandle task, int thread) {}

        @Override
        public void switchTask(TaskHandle from, TaskHandle to, int thread) {}

        @Override
        public void dequeueTask(TaskHandle task, int thread) {}

        @Override
        public void enqueueTask(TaskHandle task, int oldThread, int newThread) {}

        @Override
        public void rebalance(TaskHandle task, int srcThread, int dstThread) {}

        @Override
        public void workRequestRejected(int thread) {}

        @Override
        public void addToExtensionThread(int extensionThreadNum, int numIterations, boolean done) {}

        @Override
        public void rejectedDueToCpuBusy() {}

        @Override
        public void rejectedDueToHighLoadAverage() {}

        @Override
        public void rejectedDueToNoExtensionThreads() {}
      };

  void addTask(TaskHandle task, int thread);

  void switchTask(TaskHandle from, TaskHandle to, int thread);

  void dequeueTask(TaskHandle task, int thread);

  void enqueueTask(TaskHandle task, int oldThread, int newThread);

  void rebalance(TaskHandle task, int srcThread, int dstThread);

  void workRequestRejected(int thread);

  void addToExtensionThread(int extensionThreadNum, int numIterations, boolean done);

  void rejectedDueToCpuBusy();

  void rejectedDueToHighLoadAverage();

  void rejectedDueToNoExtensionThreads();
}
