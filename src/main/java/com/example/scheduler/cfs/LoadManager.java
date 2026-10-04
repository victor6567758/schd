package com.example.scheduler.cfs;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;

/**
 * A garbage-free structure for picking the least-loaded thread(s) - loads are bucketed in a
 * NavigableSet of same-load groups (each a small doubly-linked list of LoadEntry), so "give me
 * the threads with the minimum load" is O(1) instead of an O(numThreads) scan, and updating one
 * thread's load never allocates (the bucket nodes are pooled in freeLoadEntries).
 */
final class LoadManager {
  private final LoadEntry[] loadEntries;
  private final Deque<LoadEntries> freeLoadEntries;
  private final NavigableSet<LoadEntries> currentLoads;

  LoadManager(int numThreads) {
    loadEntries = new LoadEntry[numThreads];
    freeLoadEntries = new ArrayDeque<>();
    currentLoads = new TreeSet<>();
    for (int i = 0; i < numThreads; i++) {
      loadEntries[i] = new LoadEntry(i);
      freeLoadEntries.add(new LoadEntries());
    }
    // keep one extra to handle overlap between deleting one load and adding a new load.
    freeLoadEntries.add(new LoadEntries());
  }

  /**
   * Indicates a load change for this thread, either because new tasks became runnable or runnable
   * tasks got blocked.
   *
   * @param thread thread which saw a load change
   * @param newLoad new load on the thread in terms of number of runnable tasks
   */
  void setNewLoad(int thread, long newLoad) {
    LoadEntry entryToAdd = loadEntries[thread];
    LinkedNode entryToDelete = entryToAdd.reset();
    if (entryToDelete instanceof LoadEntries && ((LoadEntries) entryToDelete).load != newLoad) {
      currentLoads.remove(entryToDelete);
      free((LoadEntries) entryToDelete);
    }
    LoadEntries thisEntry = allocate(newLoad);
    LoadEntries foundEntry = currentLoads.floor(thisEntry);
    if (foundEntry != null && foundEntry.load == newLoad) {
      free(thisEntry);
      entryToAdd.setNewLoad(newLoad, foundEntry, foundEntry.getPrev());
    } else {
      currentLoads.add(thisEntry);
      entryToAdd.setNewLoad(newLoad, thisEntry, thisEntry.getPrev());
    }
  }

  /** Removes this load entry - typically called when the thread becomes inactive. */
  void removeLoad(int thread) {
    LoadEntry entryToAdd = loadEntries[thread];
    LinkedNode entryToDelete = entryToAdd.reset();
    if (entryToDelete instanceof LoadEntries) {
      currentLoads.remove(entryToDelete);
      free((LoadEntries) entryToDelete);
    }
  }

  /** @return the threads currently at the minimum load, as an iterable of LoadEntry. */
  Iterable<LoadEntry> getMinimumLoad() {
    return currentLoads.first();
  }

  private LoadEntries allocate(long newLoad) {
    LoadEntries next = freeLoadEntries.removeFirst();
    assert next.isEmpty();
    next.load = newLoad;
    return next;
  }

  private void free(LoadEntries item) {
    assert item.isEmpty();
    item.load = 0;
    freeLoadEntries.addLast(item);
  }

  private static final class LoadEntries extends LinkedNode
      implements Comparable<LoadEntries>, Iterable<LoadEntry> {
    private long load;

    private LoadEntries() {
      this.load = 0L;
    }

    private boolean isEmpty() {
      return super.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LoadEntries that = (LoadEntries) o;
      return load == that.load;
    }

    @Override
    public int hashCode() {
      return Objects.hash(load);
    }

    @Override
    public int compareTo(LoadEntries o) {
      return Long.compare(load, o.load);
    }

    @Override
    public Iterator<LoadEntry> iterator() {
      return new Iterator<LoadEntry>() {
        private LinkedNode current = LoadEntries.this.getNext();

        @Override
        public boolean hasNext() {
          return current != LoadEntries.this;
        }

        @Override
        public LoadEntry next() {
          LinkedNode prev = current;
          current = current.next;
          return (LoadEntry) prev;
        }
      };
    }
  }

  /** Entry for a given load. */
  static final class LoadEntry extends LinkedNode {
    private final int thread;
    private long load;

    private LoadEntry(int thread) {
      this.thread = thread;
    }

    private void setNewLoad(long load, LinkedNode head, LinkedNode tail) {
      this.load = load;
      super.attachTo(head, tail);
    }

    @Override
    public String toString() {
      return "[" + thread + "," + load + "]";
    }

    long getLoad() {
      return load;
    }

    int getThread() {
      return thread;
    }
  }

  private static class LinkedNode {
    // doubly linked list for threading all entries having same load
    private LinkedNode next;
    private LinkedNode prev;

    LinkedNode() {
      this.next = this;
      this.prev = this;
    }

    protected LinkedNode getNext() {
      return next;
    }

    protected LinkedNode getPrev() {
      return prev;
    }

    private boolean isEmpty() {
      return this.next == this;
    }

    private void attachTo(LinkedNode head, LinkedNode tail) {
      tail.next = this;
      head.prev = this;
      this.next = head;
      this.prev = tail;
    }

    LinkedNode reset() {
      LinkedNode head = null;
      if (next != this) {
        head = (next == prev) ? next : null;
        prev.next = next;
        next.prev = prev;
        next = this;
        prev = this;
      }
      return head;
    }
  }
}
