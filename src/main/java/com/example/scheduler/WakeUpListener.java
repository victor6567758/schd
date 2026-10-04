package com.example.scheduler;

/**
 * How a scheduler-side component tells its owning executor thread that work arrived while it
 * was idle.
 */
public interface WakeUpListener {
  void wakeUpIfIdle();
}
