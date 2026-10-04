package com.example.scheduler.cfs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Filters a runqueue iterator down to just its group children. */
final class Helpers {

  private Helpers() {}

  /** retrieves the list of all groups from the iterator */
  static List<GroupEntity> groupsAsList(final Iterator<Entity> iterator) {
    List<GroupEntity> groups = new ArrayList<>();
    while (iterator.hasNext()) {
      GroupEntity group = iterator.next().asGroupEntity();
      if (group != null) {
        groups.add(group);
      }
    }
    return groups;
  }
}
