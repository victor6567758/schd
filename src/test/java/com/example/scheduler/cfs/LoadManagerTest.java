package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Direct unit tests for LoadManager's bucketing: has no dependency on Entity/TaskManager at all,
 * so unlike most of the rest of cfs, it's testable in complete isolation.
 */
public class LoadManagerTest {

  private static Set<Integer> threadsAtMinimum(LoadManager mgr) {
    Set<Integer> threads = new HashSet<>();
    for (LoadManager.LoadEntry e : mgr.getMinimumLoad()) {
      threads.add(e.getThread());
    }
    return threads;
  }

  @Test
  public void singleThreadIsItsOwnMinimum() {
    LoadManager mgr = new LoadManager(1);
    mgr.setNewLoad(0, 5);

    assertEquals(Set.of(0), threadsAtMinimum(mgr));
    for (LoadManager.LoadEntry e : mgr.getMinimumLoad()) {
      assertEquals(5, e.getLoad());
    }
  }

  @Test
  public void minimumIsWhicheverThreadsHaveTheLowestLoad() {
    LoadManager mgr = new LoadManager(4);
    mgr.setNewLoad(0, 5);
    mgr.setNewLoad(1, 2);
    mgr.setNewLoad(2, 2);
    mgr.setNewLoad(3, 7);

    assertEquals(Set.of(1, 2), threadsAtMinimum(mgr));
  }

  @Test
  public void manyThreadsSharingTheSameLoadAreAllReturnedExactlyOnce() {
    LoadManager mgr = new LoadManager(5);
    for (int i = 0; i < 5; i++) {
      mgr.setNewLoad(i, 4);
    }

    assertEquals(Set.of(0, 1, 2, 3, 4), threadsAtMinimum(mgr));
  }

  @Test
  public void updatingAThreadsLoadMovesItOutOfItsOldBucket() {
    LoadManager mgr = new LoadManager(2);
    mgr.setNewLoad(0, 5);
    mgr.setNewLoad(1, 5);
    assertEquals(Set.of(0, 1), threadsAtMinimum(mgr));

    mgr.setNewLoad(0, 10); // thread 0 moves to a heavier bucket
    assertEquals(Set.of(1), threadsAtMinimum(mgr));
  }

  @Test
  public void removingAThreadExcludesItFromTheMinimum() {
    LoadManager mgr = new LoadManager(2);
    mgr.setNewLoad(0, 1);
    mgr.setNewLoad(1, 1);

    mgr.removeLoad(0);

    assertEquals(Set.of(1), threadsAtMinimum(mgr));
  }

  @Test
  public void repeatedlySettingTheSameLoadIsIdempotent() {
    LoadManager mgr = new LoadManager(2);
    mgr.setNewLoad(0, 3);
    mgr.setNewLoad(0, 3);
    mgr.setNewLoad(0, 3);
    mgr.setNewLoad(1, 9);

    assertEquals(Set.of(0), threadsAtMinimum(mgr));
  }
}
