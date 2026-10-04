package com.example.scheduler.cfs;

import com.example.scheduler.TaskHandle;

/**
 * What an executor thread actually calls, regardless of whether it's running a regular thread
 * queue or one borrowed from the extension pool.
 */
interface ThreadScheduler {
  TaskHandle getTask(long time);
}
