package com.example.scheduler.cfs;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper around a concurrent queue that tracks its size with an atomic, so size() doesn't
 * have to walk the queue.
 */
final class DiffQueue<T> {
  private final Queue<T> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger size = new AtomicInteger();

  void add(T e) {
    queue.add(e);
    size.incrementAndGet();
  }

  boolean contains(T e) {
    return queue.contains(e);
  }

  int size() {
    return size.get();
  }

  T poll() {
    T ret = queue.poll();
    if (ret != null) {
      size.decrementAndGet();
    }
    return ret;
  }
}
