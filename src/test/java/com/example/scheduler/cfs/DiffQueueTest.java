package com.example.scheduler.cfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Direct unit tests for DiffQueue: a plain queue with an atomic, always-accurate size. */
public class DiffQueueTest {

  @Test
  public void startsEmpty() {
    DiffQueue<String> q = new DiffQueue<>();
    assertEquals(0, q.size());
    assertNull(q.poll());
  }

  @Test
  public void addIncrementsSizePollDecrementsIt() {
    DiffQueue<String> q = new DiffQueue<>();
    q.add("a");
    q.add("b");
    assertEquals(2, q.size());

    assertEquals("a", q.poll());
    assertEquals(1, q.size());
    assertEquals("b", q.poll());
    assertEquals(0, q.size());
  }

  @Test
  public void pollingAnEmptyQueueReturnsNullAndDoesNotUnderflowSize() {
    DiffQueue<String> q = new DiffQueue<>();
    assertNull(q.poll());
    assertEquals(0, q.size());
  }

  @Test
  public void preservesFifoOrder() {
    DiffQueue<Integer> q = new DiffQueue<>();
    for (int i = 0; i < 5; i++) {
      q.add(i);
    }
    for (int i = 0; i < 5; i++) {
      assertEquals(Integer.valueOf(i), q.poll());
    }
  }

  @Test
  public void containsFindsAddedElementsUntilPolled() {
    DiffQueue<String> q = new DiffQueue<>();
    q.add("x");
    assertTrue(q.contains("x"));
    assertFalse(q.contains("y"));

    q.poll();
    assertFalse(q.contains("x"));
  }
}
