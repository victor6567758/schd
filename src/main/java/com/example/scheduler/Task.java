package com.example.scheduler;

/**
 * The minimal contract the scheduler needs from whatever it's scheduling - just its current
 * state. Everything about what a task actually does is the scheduler's business to not know
 * about.
 */
public interface Task {

  enum State {
    RUNNABLE,
    BLOCKED,
    DONE
  }

  State getState();
}
