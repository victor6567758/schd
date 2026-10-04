# Round Robin vs. CFS: a scheduler learning project

This is a standalone Java project built to answer one question concretely,
in code rather than slides: what does a real CPU scheduler actually do
differently from the naive approach, and why? It implements two schedulers
side by side - a naive round-robin scheduler and a much deeper
CFS-style scheduler - and tests both against the same kind of fake
workload, so the difference in behavior is something you can run and
watch, not just read about.

This document walks through how each piece works and why it's built the
way it is.

## Contents

- [The baseline: round robin](#the-baseline-round-robin)
- [CFS's core idea, the simple version](#cfss-core-idea-the-simple-version)
  - [Then where does priority come in?](#then-where-does-priority-come-in)
  - [Watching it happen: two tasks, weights 2 and 1](#watching-it-happen-two-tasks-weights-2-and-1)
  - [The new-task problem, and `min_vruntime`](#the-new-task-problem-and-min_vruntime)
  - [The state machine underneath every `Entity`](#the-state-machine-underneath-every-entity)
- [Strict priority as an alternative policy: `WeightBasedTaskEntity`](#strict-priority-as-an-alternative-policy-weightbasedtaskentity)
- [Group scheduling: a group is just another entity](#group-scheduling-a-group-is-just-another-entity)
  - [The two-level hierarchy: `TaskGroup` and `GroupEntity`](#the-two-level-hierarchy-taskgroup-and-groupentity)
  - [Submitting a task into a group](#submitting-a-task-into-a-group)
  - [Double vruntime accounting](#double-vruntime-accounting)
  - [Fairness across threads: `groupPenalty`](#fairness-across-threads-grouppenalty)
- [Supporting classes at a glance](#supporting-classes-at-a-glance)
- [Making it multi-threaded](#making-it-multi-threaded)
  - [Per-thread scheduling: the two-phase protocol](#per-thread-scheduling-the-two-phase-protocol)
  - [Cross-thread coordination: staging, requests, and placement](#cross-thread-coordination-staging-requests-and-placement)
  - [The executor loop](#the-executor-loop)
- [Blocking and re-enqueue](#blocking-and-re-enqueue)
- [RPC-offload: batching re-enqueue onto one thread](#rpc-offload-batching-re-enqueue-onto-one-thread)
- [Extension pool: borrowing idle host capacity](#extension-pool-borrowing-idle-host-capacity)
- [Observability](#observability)
- [Testing approach](#testing-approach)
- [Build and test](#build-and-test)
- [Further reading](#further-reading)

## The baseline: round robin

The round-robin scheduler (`roundrobin/RoundRobinScheduler.java`) is
deliberately dumb: tasks sit in a plain FIFO queue, and each gets exactly one
fixed-size time quantum before going to the back of the line.

```java
SimTask task = queue.pollFirst();
task.runFor(quantum);
if (!task.isDone()) {
  queue.addLast(task);
}
```

That's the entire scheduling policy. It has no concept of priority - a task
declared as ten times as important as another gets treated identically. Its
only notion of fairness is turns taken, not CPU time consumed. That's the
contrast the rest of this project exists to make concrete.

The tasks are real code. `runFor(quantum)` isn't just decrementing a
counter. A `SimTask` is built from an amount of work (a number of ticks) and
a `Runnable` that is executed once per tick, on whichever thread the
scheduler runs the slice on:

```java
List<String> log = new ArrayList<>();
scheduler.submit(new SimTask("A", 4, () -> log.add("A")));
scheduler.submit(new SimTask("B", 4, () -> log.add("B")));
// with a quantum of 2, the log ends up as A A B B A A B B
```

The same `SimTask` is what the CFS scheduler runs, so both schedulers can be
watched doing actual work - appending to a log, summing numbers, recording
which thread they ran on - and the tests check those real results, not only
the bookkeeping.

## CFS's core idea, the simple version

Picture a daycare with one swing and a line of kids waiting. The fair way to
run it: give every kid a personal stopwatch that only ticks while they're on
the swing. Whenever the swing frees up, whichever kid's stopwatch reads the
least goes next. No schedule, no turns-in-order - just "who's had the
least so far goes now." Run that rule forever and everyone converges on an
equal share, automatically, without anyone ever counting turns or planning
ahead.

CFS runs a CPU the same way. The "stopwatch reading" is called virtual
runtime (`vruntime`), and the entire scheduling rule really is that simple:

> Always run whichever runnable task has the smallest vruntime.

### Then where does priority come in?

Here's the part that isn't obvious from the swing analogy: what if some kids'
stopwatches run at half speed - a full minute on the swing only ticks their
stopwatch by 30 seconds? They'll read "least time so far" much more often,
so they get called back to the swing much more often, even though the "pick
whoever's lowest" rule never changed at all.

That's exactly the trick. CFS never asks "who's more important" when
choosing who runs - it only ever asks "whose vruntime is smallest." Priority
is smuggled in entirely through how fast each task's own vruntime ticks.
Every time a task runs for some duration, its vruntime advances by:

```
delta = duration * load / weight
```

`load` is the sum of the weights of everything currently competing at that
level, `weight` is the task's own priority. Double a task's weight and its
stopwatch runs at half speed: same picking rule, same code path, but now it
reads "smallest" roughly twice as often and ends up with roughly twice the
real CPU time. (Real Linux CFS does the same thing with `nice` values -
`nice 0` maps to weight 1024, and each step of `nice` scales the weight by
~1.25x, so a `nice +5` task's vruntime ticks about 3x faster than a `nice 0`
task's.)

This single formula, implemented once in `Entity.updateVRuntime`, is reused
unchanged by every layer built on top of it (tasks, groups, weighted
variants) - the whole point of modeling priority as a rate-of-vruntime-growth
rather than as a queue position.

`RunQueue` is the data structure this rule runs on: a priority queue of
`Entity` objects ordered by vruntime, tracking the sum of member weights
(`load`) and the queue's `minVRuntime`. (Real CFS uses a red-black tree for
this instead of a plain priority queue, for O(log n) insert/remove at scale -
same idea, different data structure.)

### Watching it happen: two tasks, weights 2 and 1

Formulas are one thing; here's what actually happens, traced from a real run
of two tasks - `A` with weight 2, `B` with weight 1 - each asking for a
10-tick slice every turn. `load = 2 + 1 = 3` throughout.

| turn | who ran | delta = ceil(10 × 3 / weight) | A.vruntime | B.vruntime | next pick |
|---|---|---|---|---|---|
| 1 | A | ceil(30/2) = 15 | 15 | 0  | B (0 < 15) |
| 2 | B | ceil(30/1) = 30 | 15 | 30 | A (15 < 30) |
| 3 | A | 15              | 30 | 30 | B (tie → B) |
| 4 | B | 30              | 30 | 60 | A (30 < 60) |
| 5 | A | 15              | 45 | 60 | A (45 < 60) |
| 6 | A | 15              | 60 | 60 | B (tie → B) |

After 6 turns: A has run 4 times (40 ticks), B has run 2 times (20 ticks) -
exactly the 2:1 ratio their weights predict. The mechanism producing that
ratio is entirely local and myopic - nothing here computes "give A twice as
much CPU" as a goal; it falls out of A's vruntime simply growing at half the
rate every time it runs, so it keeps re-qualifying as "smallest vruntime"
about twice as often. (You can reproduce this exact trace: `TaskManager
manager = new TaskManager(1); TaskGroup g = manager.newGroup(1, false);` then
add two `SimTask`s of weight 2 and 1 and drive `manager.getThreadScheduler(0,
...)` by hand, the same way `CfsSchedulerTest.Driver` does.)

### The new-task problem, and `min_vruntime`

A freshly submitted task naturally starts at `vruntime = 0`. If everything
else in the queue has already accumulated, say, 50,000 ticks of vruntime, the
new task would dominate the CPU until it "catches up" - which could take a
very long time. The fix is `min_vruntime`: track the smallest vruntime
currently in the queue, and give every new (or returning) entity that value
as its starting point instead of zero.

```java
void enqueue(Entity entity) {
  queue.add(entity);
  ...
}

void putBack(Entity entity) {
  queue.add(entity);
  minVRuntime = Math.max(minVRuntime, peek().getVRuntime());
}
```

`CfsSchedulerTest#minVRuntimeStopsALateJoinerFromMonopolizingTheThread` is the
test that exercises this directly: run two tasks for a while, add a third,
and confirm the newcomer neither starves (gets close to its fair 1/3 share
immediately) nor monopolizes (no run of more than two consecutive turns).

### The state machine underneath every `Entity`

Every `TaskEntity` (and every `GroupEntity`) is one of four states, and it's
worth being precise about them because several other pieces of this project -
`ThreadMQ`'s staging, `groupPenalty`'s asymmetry - only make sense once this
is clear:

```
        addToRunQueue()             prepareToRun()
DEQUEUED ─────────────────► ENQUEUED ─────────────────► RUNNING_DEQUEUED
   ▲                                                          │
   │            removeFromRunQueue()                         │ prepareToStop(duration)
   └──────────────────────────────────────────────────────────┘
                    (back to ENQUEUED, vruntime updated)
```

- DEQUEUED: not in any runqueue. Where every entity starts, and where a
  blocked or finished task ends up.
- ENQUEUED: sitting in its parent's `RunQueue`, waiting to be picked.
- RUNNING_DEQUEUED: currently executing - and, importantly, physically
  removed from the runqueue while it runs (`prepareToRun()` calls
  `removeRunning()`). This is why a picker never has to skip over "the thing
  that's already running."
- RUNNING_ENQUEUED: `GroupEntity`-only. A group takes this state instead
  of `RUNNING_DEQUEUED` when one of its own members is running but it still
  has other runnable members - so, unlike a plain running task, it stays
  visible in its parent's queue the whole time. `EntityStateMachineTest`
  traces a `TaskEntity` through this exact cycle, including what blocking
  does (jumps straight to `DEQUEUED`, skipping past `ENQUEUED`).

Whichever branch a `GroupEntity` takes here is also the reason `groupPenalty`
behaves asymmetrically for multi-member vs. single-member groups - see the
groupPenalty section below.

## Strict priority as an alternative policy: `WeightBasedTaskEntity`

Proportional sharing is the default, but it isn't always what you want - a
UI-facing query shouldn't wait behind a background job just because they're
otherwise "fairly" splitting CPU. `WeightBasedTaskEntity` is a small subclass
that changes the ordering rule, not the vruntime math:

```java
@Override
public int compareTo(Entity o) {
  int compareWeight = -1 * Long.compare(getWeight(), o.getWeight());
  return (compareWeight != 0) ? compareWeight : super.compareTo(o);
}
```

Higher weight always wins, full stop - vruntime only breaks ties within the
same weight class. This is sometimes called a "scheduling class": a way to
guarantee one class of task is never preempted by a lower-priority one, while
tasks of equal priority still get ordinary proportional fairness among
themselves. `WeightBasedSchedulingTaskGroup` is the group-level version of the
same idea - swap in `WeightBasedTaskEntity` instead of the plain kind when a
group's tasks are created.

## Group scheduling: a group is just another entity

The next problem: what stops a single tenant from grabbing more than their
fair share simply by spawning more tasks? The answer is to make a group of
tasks compete at the parent level as if it were one entity, with its own
vruntime, while its members compete with each other independently inside
it.

### The two-level hierarchy: `TaskGroup` and `GroupEntity`

`TaskGroup` and `GroupEntity` split this into two cooperating classes:

- `TaskGroup` represents the group as a whole, across every thread. It
  holds one `GroupEntity` per thread (`entities[thread]`) and a single
  `cRuntime` counter shared by all of them.
- `GroupEntity` is the group's local presence on one specific thread.
  It is itself an `Entity` - it has its own weight and vruntime, and sits in
  its parent's `RunQueue` exactly like a plain task would - but it also owns
  its own `RunQueue`, holding whichever tasks (or nested groups) were
  placed on that thread.

```
root
 ├─ GroupEntity "tenant-a" @ thread 0  (own RunQueue: task1, task2, ...)
 ├─ GroupEntity "tenant-a" @ thread 1  (own RunQueue: task3)
 ├─ GroupEntity "tenant-b" @ thread 0  (own RunQueue: task4)
 └─ TaskEntity  "ungrouped-task"       @ thread 0
```

The two "tenant-a" boxes are different objects - different runqueues,
different vruntime - but they share the same parent `TaskGroup`, and
therefore the same `cRuntime`. That shared counter is what makes cross-thread
fairness possible at all (see `groupPenalty` below).

### Submitting a task into a group

`TaskGroup.addTask(task, weight)` does three things: picks a thread (via the
manager's `pickLeastBusy()`/`pickLeastBusyInitialWeighted()` - the same
load-balancing logic a bare task would go through), wraps the task in a
`TaskEntity` whose parent is that thread's `GroupEntity` (not the root),
and hands it to the manager to stage. From that point on the task only ever
competes directly against its siblings inside the group - the root runqueue
never sees it, only the `GroupEntity` that represents the whole group.

### Double vruntime accounting

Every time a task inside a group runs, its `prepareToStop()` call cascades
upward through the state machine described earlier - except now there are
two levels of accounting instead of one:

1. The task's own vruntime advances against the group's internal load
   (the summed weight of its siblings inside the same group) - this is what
   makes tasks within a group share fairly among themselves.
2. The group's own vruntime also advances, by that same duration,
   against the parent's load (the group's own siblings, one level up) -
   this is what makes the group as a whole compete fairly against its
   siblings, no matter how many tasks happen to be inside it.

That second step is why a group with five tasks doesn't get five times the
CPU of a group with one: however many tasks are running underneath it, the
group's own vruntime only ticks once per task-run, at the same rate a
single task's would.
`CfsSchedulerTest#twoSingleTaskGroupsOfEqualWeightSplitCpuEvenly` confirms
this for the simplest case: two single-task groups of equal weight on one
thread split CPU exactly 50/50.

### Fairness across threads: `groupPenalty`

The double-accounting above is exact as long as a group's tasks all sit on
the same thread. Once a scheduler spans multiple threads, a group's tasks
can end up spread unevenly - some on a busy thread, some on an idle one -
and no single thread's local vruntime math can see that, because each thread
only ever looks at its own `GroupEntity` and its own local competitors.

`GroupEntity.updateShares()` is the correction. It first calls the tiny
`Helpers.groupsAsList()` utility to pull just the `GroupEntity` children back
out of the current runqueue's iterator - a plain runqueue mixes groups and
ungrouped tasks together, and this is the one line of filtering that
separates them out. With that list of sibling groups in hand, `updateShares()`
sums their `cRuntime` (shared across all of that group's threads, per above),
and divides by the number of active siblings to get each group's fair
share of the combined total. A
group whose `cRuntime` is above that share gets `groupPenalty = ceil(cRuntime
/ share)` (capped at `MAX_GROUP_PENALTY`); everyone else keeps
`groupPenalty = 1`. The penalty then feeds into `getFairWeight()`:

```java
double getFairWeight() {
  return getWeight() / (double) groupPenalty;
}
```

Dividing weight by penalty makes a group's local fairWeight smaller
whenever it's running ahead overall - which, by the same "bigger weight →
more CPU" rule from earlier, makes its vruntime grow faster on this
thread, so it gets picked less often here to compensate for the extra CPU
it's getting elsewhere. `getFairWeight()` returns a `double` rather than the
`long` every other weight in this project uses, specifically because integer
division would truncate `1 / 2` straight to `0` for the common unit-weight
case - silently cancelling the correction for exactly the groups it matters
most for.

One structural detail is worth knowing before relying on this: `updateShares()`
only ever sees groups that are physically still sitting in the runqueue at
that moment, and whether a group's `GroupEntity` stays there while one of
its own members runs depends on whether it has other runnable members
left. A group with more than one runnable task takes the `RUNNING_ENQUEUED`
branch and stays visible; a group (or plain task) with none takes the
`RUNNING_DEQUEUED` branch and is removed, exactly like an ordinary task
while it runs. Two single-member groups are always symmetric here, so
`groupPenalty` never engages between them - a clean 50/50, confirmed by
`CfsSchedulerTest#twoSingleTaskGroupsOfEqualWeightSplitCpuEvenly` above. A
multi-member group compared against a single-member sibling is not
symmetric: the multi-member group can only ever be compared right after its
own turn, never the reverse. The practical consequence: "grouping N tasks
together gets exactly the same total CPU as a single task" is not a clean
invariant of this algorithm once `groupPenalty` can engage. What does hold,
and what
`CfsSchedulerTest#groupPenaltyReducesAGroupsLocalShareToCompensateForExtraCpuElsewhere`
tests, is the weaker, honest claim: a group's local share on a contested
thread measurably drops (from a naive 1/3 down below 1/4 in that test) to
partially compensate for CPU it's getting elsewhere - not that the aggregate
ratio converges to exactly 1.

## Supporting classes at a glance

The sections above covered the core idea - vruntime, the weight formula,
groups - using only a handful of classes. From here on, making that idea
work across multiple threads pulls in a lot more of them, so this is a
quick-reference table for the rest of `cfs/`: one or two sentences on what
each class is actually responsible for, grouped by the layer it belongs to,
plus a short paragraph per group on how they work together. The sections
after this one tell the fuller story of why each exists - come back to
this table any time a class name shows up later and it's not obvious what
it does.

The entity hierarchy - what gets scheduled

| Class | Responsible for |
|---|---|
| `Entity` | The vruntime state machine (DEQUEUED/ENQUEUED/RUNNING_*) and the core `delta = duration * load / weight` formula. Everything else in this hierarchy is a subclass or a collaborator of this one class. |
| `TaskEntity` | Wraps one `SimTask` as a leaf `Entity` - the thing that's actually runnable. |
| `WeightBasedTaskEntity` | A `TaskEntity` whose `compareTo` checks weight before vruntime - the strict-priority variant. |
| `GroupEntity` | One group's local presence on one specific thread: owns its own `RunQueue` of members, and is itself an `Entity` in its parent's `RunQueue`. |
| `TaskGroup` | The group as a whole, spanning every thread: holds one `GroupEntity` per thread plus the `cRuntime` counter they share. Also where `addTask()`/thread-placement happens. |
| `WeightBasedSchedulingTaskGroup` | A `TaskGroup` whose members are `WeightBasedTaskEntity` instead of the plain kind. |
| `RunQueue` | The priority queue itself: orders `Entity` objects by vruntime, tracks `load` (summed weights) and `minVRuntime`. Used identically at every level - root, group, or nested group. |
| `Helpers` | A one-method utility class: `groupsAsList()` walks a runqueue's iterator and returns just the entries that are `GroupEntity` (skipping plain `TaskEntity` ones). It exists purely so `updateShares()` (below) doesn't have to repeat that filtering logic itself. |

How they fit together: `Entity` is the one place the vruntime state machine
and the `delta = duration * load / weight` formula live - every row below it
is either a leaf standing in for real work (`TaskEntity`, and its
strict-priority variant `WeightBasedTaskEntity`) or a way of making a whole
group of tasks compete as a single entity (`GroupEntity`/`TaskGroup`,
and their weighted variant). `RunQueue` is the ordered structure all of them
sit in, at every level of nesting; `Helpers` is a small filtering step
`GroupEntity` needs when it compares itself against its siblings. The
[CFS core idea](#cfss-core-idea-the-simple-version) section walks through the
formula itself; [group scheduling](#group-scheduling-a-group-is-just-another-entity)
covers how the two-level split works.

Per-thread scheduling - "what runs next, on this one thread"

| Class | Responsible for |
|---|---|
| `TaskProvider` (interface) | The contract for "pop the next task off this thread's runqueue": `taskDone()`/`getTask()`'s two-phase protocol, plus `getTaskToMigrate()` for handing a task to another thread. |
| `DefaultTaskProvider` | The plain implementation - not thread-safe, because normally only the owning thread ever touches it. |
| `ThreadSafeTaskProvider` | The same logic, `synchronized`, for the one case where another thread does need to reach in concurrently (see `EagerTaskSheddingThreadMQ` below). |
| `ThreadScheduler` (interface) | The even thinner contract (`getTask(time)`) that lets `SlicingThread` talk to either a `ThreadMQ` or an `ExtensionMQ` without caring which. |

How they fit together: `TaskProvider` is the two-method contract
(`getTask()`/`taskDone()`) that turns "pick the smallest vruntime" into an
actual run-one-task-at-a-time loop; `DefaultTaskProvider` is the everyday,
non-synchronized implementation, and `ThreadSafeTaskProvider` is the same
logic with `synchronized` added, for the one case where a second thread
needs to reach in. `ThreadScheduler` sits a level above `TaskProvider`: it's
what `SlicingThread` (the actual worker loop) calls each tick, and it's
deliberately generic enough that the worker doesn't need to know whether
it's driving a normal thread's `ThreadMQ` or a borrowed `ExtensionMQ`.
Traced step by step in
[Per-thread scheduling](#per-thread-scheduling-the-two-phase-protocol).

Cross-thread coordination

| Class | Responsible for |
|---|---|
| `ThreadMQ` | One thread's mailbox: the `staging`/`requests` queues, `handleMessages()` to drain them, and the idle/proactive work-request logic. |
| `EagerTaskSheddingThreadMQ` | A `ThreadMQ` that hands work over the instant a request arrives, instead of waiting until it's idle - needs `ThreadSafeTaskProvider` because of the resulting concurrent access. |
| `LoadManager` | The garbage-free "which thread(s) currently have the lowest load" structure `TaskManager` uses for placement decisions. |
| `TaskManager` | The top-level coordinator: owns every `ThreadMQ`, decides where new/unblocked tasks land, and holds the one real lock everything else assumes is already taken. |
| `SlicingThread` | The actual worker thread: ask `ThreadScheduler` for a task, run it for a slice, report back, repeat. |

How they fit together: every worker thread gets its own `ThreadMQ`, which
behaves like a mailbox rather than a shared object - it holds a `staging`
queue for tasks handed to it and a `requests` queue for other threads asking
for work, and only ever drains those queues itself (`EagerTaskSheddingThreadMQ`
is the variant that can respond to a request immediately instead of waiting
to go idle). No thread ever reaches into another thread's runqueue directly.
`TaskManager` is the coordinator sitting above all of the `ThreadMQ`s: it
decides where a new or unblocked task lands, using `LoadManager` (a structure
built to answer "which thread has the least load" without scanning every
thread) to find candidates, and it holds the one lock all of that placement
logic assumes is already taken. `SlicingThread` is the loop that actually
drives a thread forward: ask, run, report, repeat. See
[Cross-thread coordination](#cross-thread-coordination-staging-requests-and-placement)
and [the executor loop](#the-executor-loop) for the full walkthrough,
including a traced example handoff.

RPC-offload

| Class | Responsible for |
|---|---|
| `DiffQueue` | A tiny wrapper used throughout this package: a `ConcurrentLinkedQueue` (safe for multiple threads to add/poll without locking) plus an `AtomicInteger` that's incremented/decremented alongside every add/poll, so `size()` is an O(1) read instead of the O(n) walk a plain queue would need. |
| `DeferredTaskQueue` | Collects re-enqueue requests from many callers and delivers them to a handler in one batch. |
| `ReEnqueueThread` | The one dedicated thread that drains `DeferredTaskQueue` in a loop. |
| `OffloadingTaskManager` | A `TaskManager` that routes `reEnqueueTaskEntity()` through the two classes above instead of doing it inline. |

How they fit together: this group solves one specific problem - many
independent caller threads (RPC event threads, say) all wanting the
`TaskManager`'s lock at the same moment to re-enqueue a task that just
unblocked. Instead of every caller contending for that lock directly, each
one drops its entity onto a `DeferredTaskQueue` (backed by the lock-free
`DiffQueue`) and returns immediately, without ever blocking. `ReEnqueueThread`
is the single dedicated thread that drains the queue in bulk and performs
the real, lock-holding re-enqueue for each entry, in order - so only that one
thread ever waits on the lock, no matter how many callers there were.
`OffloadingTaskManager` is simply the `TaskManager` that wires this path in
instead of re-enqueuing inline. Full trace in
[RPC-offload](#rpc-offload-batching-re-enqueue-onto-one-thread).

Extension pool

| Class | Responsible for |
|---|---|
| `RunQueueExtender` | The policy: is the extension pool enabled, does the host have spare capacity right now, is a free extension thread actually available - and if all three, which one to hand out. |
| `ExtensionMQ` | The per-extension-thread mailbox - holds exactly one task at a time, and hands it back to `RunQueueExtender` when it's done, blocked, or the extender wants it back. |

How they fit together: `RunQueueExtender` is the gatekeeper - before a task
is allowed anywhere near the extension pool, it checks three independent
things: is the task itself eligible and the pool enabled, does the host
actually have spare CPU capacity right now, and is a free extension thread
even available. Only if all three pass does it hand the task off. `ExtensionMQ`
is what the task lands on: the simplest possible `ThreadScheduler`, since a
borrowed thread doesn't need a real runqueue - it just holds the one task
until it finishes, blocks, or `RunQueueExtender` decides to take it back. See
[Extension pool](#extension-pool-borrowing-idle-host-capacity) for the three
gates in detail.

Observability

| Class | Responsible for |
|---|---|
| `BasicStatsObserver` | Passive stats collection - reacts to events `TaskManager` reports (task added, migrated, rejected, ...) with no scheduling logic of its own. |

How they fit together: `TaskManager` and `ThreadMQ` call into a small
`Observer` hook interface (not shown as its own row above, since it has no
scheduling responsibility of its own) whenever something worth recording
happens - a task added, migrated, or rejected. `BasicStatsObserver` is the
one real implementation, purely counting what it's told; nothing it does
ever feeds back into a scheduling decision, which is what lets a no-op
observer be the default with zero behavioral difference. See
[Observability](#observability) for the full hook list.

## Making it multi-threaded

A single runqueue only uses one CPU core. `TaskManager` owns one `ThreadMQ`
per worker thread, each wrapping its own `RunQueue`-backed hierarchy
(everything in the two sections above, replicated once per thread), and
coordinates between them. Two separate concerns are worth pulling apart:
what happens inside one thread's own runqueue (unchanged from everything
already described), and how tasks actually get from one thread's runqueue
to another's.

### Per-thread scheduling: the two-phase protocol

Zoom into a single thread and forget about the others for a moment:
something still has to decide "what runs next" and account for whatever
just finished. That's `TaskProvider`'s job - a small interface with three
methods: `taskDone(time)`, `getTask()`, `getTaskToMigrate()`.

`DefaultTaskProvider` is the plain implementation, and every call to it
follows the same two-step sequence:

1. `taskDone(time)` computes how long the previous task ran (`time` minus
   the timestamp of the last call) and calls `prepareToStop(duration)` on
   it - the vruntime update from the CFS section above, applied to whichever
   entity was running.
2. `getTask()` then picks the new leftmost entity from the runqueue and
   calls `prepareToRun()` on it.

`ThreadMQ.getTask(time)` always calls these in that order - accounting for
the previous task before picking the next one - which is exactly the
sequence `SlicingThread`'s loop drives every iteration: run for a slice,
then ask again, passing back how much virtual time just elapsed.
`getTaskToMigrate()` is the other half of the interface: instead of running
the leftmost entity, pull it out of the runqueue entirely
(`removeFromRunQueue()`) and hand it to whoever asked - this is the
mechanism cross-thread load balancing (below) actually moves work with.

Why two implementations of the same interface? `DefaultTaskProvider`
assumes only the owning thread ever calls it - true almost everywhere, since
a runqueue is normally one thread's private view of its own tasks. The one
exception is `EagerTaskSheddingThreadMQ`, which lets an idle thread call
`getTaskToMigrate()` on a busy thread's provider concurrently with that
busy thread's own `getTask()`/`taskDone()` calls. `ThreadSafeTaskProvider`
is the identical logic with `synchronized` on every method, used only in
that one case.

### Cross-thread coordination: staging, requests, and placement

- New task placement: `pickLeastBusy()` (plain groups) is a straight
  linear scan for whichever thread's `getLoad()` is currently smallest.
  `pickLeastBusyWeighted()` (weight-based groups, and re-enqueuing an
  unblocked entity) is choosier: it first narrows down to the thread(s) at
  the overall minimum load using `LoadManager` - a garbage-free structure
  that buckets threads by load in a sorted set of same-load groups, so
  "who's currently lowest" is a cheap lookup instead of an O(numThreads)
  scan - and then, among ties, picks whichever thread already has the least
  additional relative weight competing there, so a heavy task doesn't
  land on a thread that looks quiet right now but is about to get busy from
  a sibling task with much higher priority.
- Cross-thread handoff: each `ThreadMQ` has a `staging` queue (tasks
  handed to this thread but not yet in its runqueue) and a `requests` queue
  (other threads asking for work). `handleMessages()`, called on every
  scheduling tick, drains both. Crucially, no thread ever reaches into
  another thread's runqueue directly - the only cross-thread interaction is
  dropping something into these queues, which is why a plain
  `ConcurrentLinkedQueue`-backed `DiffQueue` is enough; the actual
  `RunQueue`/`Entity` machinery stays single-threaded from its own point of
  view.
- Idle work-stealing: when a thread's runqueue goes empty,
  `requestMoreWork()` finds the busiest other thread and asks it for a task.
  If `onIdleLoadShed` is enabled, a busy thread can also proactively shed
  work the moment it notices a pending request, rather than waiting to be
  asked while already idle - `EagerTaskSheddingThreadMQ` is the more
  aggressive variant that does this via a thread-safe runqueue provider.

Here's the whole handoff sequence, traced from a real run - two tasks
pinned to thread 0, `onIdleLoadShed` on, thread 1 empty:

```
after submit:            thread0 staged=2 tasks=0 | thread1 staged=0 tasks=0
thread0 drains staging:  thread0 staged=0 tasks=2
thread1 finds nothing, requests work from thread0
                         thread0 pending requests=1
thread0 services the request (sheds one task, since 2 > 1 remain):
                         thread0 tasks=1 | thread1 staged=1
thread1 drains its own staging:
                         thread1 tasks=1
```

Notice the request and the actual migration are two separate ticks: thread 1
asking doesn't immediately move anything (thread 0 might be mid-slice and
unaware); the hand-off only happens the next time thread 0's own
`getTask()` runs and finds a pending request in its `requests` queue. And
even then, the task lands in thread 1's staging, not its runqueue directly
- thread 1 still has to drain it on its own next tick. Every step is a
message drop, never a direct read of someone else's data structure.

### The executor loop

`SlicingThread` is the actual worker: a loop that asks its `ThreadScheduler`
for the next task, runs it for one slice, and reports back how long that
took - which, per the two-phase protocol above, is also when the previous
task's vruntime actually gets updated. It runs on a virtual tick counter
instead of real `System.nanoTime()`, specifically so tests are deterministic
instead of racing real wall-clock timing.

Most of the CFS tests don't spin up real threads at all - they drive a
`ThreadScheduler` by hand from the test thread with a manually-incremented
tick counter (see `CfsSchedulerTest.Driver`), exactly reproducing what
`SlicingThread` would do, but reproducibly. Only one test per scheduler
(`multiThreadedExecutionRunsEveryTaskExactlyOnce`) uses real background
threads, specifically to prove the locking itself is correct under genuine
concurrency - everything else is deliberately deterministic.

## Blocking and re-enqueue

A task can go from `RUNNABLE` to `BLOCKED` (simulating waiting on I/O).
`ThreadMQ` notices this the next time it's asked for a task, dequeues it, and
it's up to whatever unblocked the task to call
`TaskHandle#reEnqueue()`. That call goes through `TaskManager
.reEnqueueTaskEntity()`, which - if `rescheduleOnUnblock` is set - picks a
new thread for it rather than assuming it should go back to the same one,
using the same weighted/unweighted placement logic as a brand-new task.

## RPC-offload: batching re-enqueue onto one thread

`TaskManager.reEnqueueTaskEntity()` - the method that runs whenever a
blocked task unblocks and asks to be re-enqueued - needs the manager's
mutex, because it reads and mutates shared placement state
(`pickLeastBusy`/`pickLeastBusyWeighted`, `LoadManager`). In a real system,
the callers triggering that unblock are often many independent,
latency-sensitive threads - RPC event threads reacting to network I/O
completing, say - and if they all call `reEnqueueTaskEntity()` directly,
they queue up one-at-a-time waiting for that same lock: the work still gets
done in the same order eventually, just with every caller thread blocked
and waiting its turn instead of getting on with whatever it was doing.

`OffloadingTaskManager` breaks that queue-behind-a-lock pattern without
changing the outcome. Instead of taking the mutex itself,
`reEnqueueTaskEntity()` just drops the entity onto a `DeferredTaskQueue`
(`nonBlockingAdd` - a lock-free concurrent queue) and returns immediately;
the calling thread never blocks on anything. One dedicated background
thread, `ReEnqueueThread`, does nothing but loop calling
`processBulkEnqueue`, which drains everything that's piled up into a
priority queue (ordered the same way a runqueue would be) and hands the
whole batch to a handler that calls the real `reEnqueueTaskEntity()` - the
one that takes the mutex - for each entry, in order, one at a time.

The net effect is identical to every caller taking the mutex directly and
re-enqueuing one at a time, which is what still happens - just now inside
`ReEnqueueThread`, on one thread, instead of on however many callers' own
threads happened to trigger it. Nobody but that one background thread ever
waits on the lock.

## Extension pool: borrowing idle host capacity

Sometimes every regular thread looks loaded, but the host still has spare
CPU capacity sitting idle - more cores than configured threads, say. The
extension pool is a small number of extra worker threads that only get used
for tasks that specifically opt in, and only when there's genuinely nowhere
else for them to go.

Three separate gates all have to open before a task is routed there, all
checked inside `RunQueueExtender.selectExtensionPool()`, which
`TaskManager.pickLeastBusyWeighted()` calls only when every regular
thread it scanned already looked loaded, and only for weight-based groups
being re-enqueued after unblocking (initial placement never considers the
extension pool at all):

1. The task itself has to be eligible, and the pool has to be enabled.
   Both are plain flags - `SimTask.isEligibleForExtensionPool()` and an
   `enable()`/`disable()` toggle on `RunQueueExtender`.
2. The host has to actually have spare capacity right now - checked
   against two independently injected `DoubleSupplier`s (current CPU load,
   and 1-minute load average) rather than real OS calls, specifically so a
   test can simulate "the host is busy" or "the host has room" without
   depending on whatever machine happens to be running it. If the CPU load
   supplier reports at or above a threshold, or the load average was
   recently reported "high", the request is rejected outright; recovery
   from a high load average is just checked again in-line the next time
   anyone asks, rather than by a dedicated background thread polling for it.
3. A free extension thread actually has to exist - tracked in a plain
   `DiffQueue<Integer>` of thread IDs, populated once per extension thread
   the first time it registers itself, and drained here.

If all three pass, the task migrates via `runInExtensionPool` into
`ExtensionMQ.add()`, onto whichever extension thread was free.
`ExtensionMQ` is deliberately the simplest possible `ThreadScheduler`: it
holds exactly one task and has no runqueue at all. Its `getTask()` keeps
handing back the same task's handle every call until one of three things
happens - the task finishes, the task blocks, or
`RunQueueExtender.handOverTaskEntityIfBusy()` decides the host has gotten
busy again since the task arrived and routes it back through the regular
scheduling path instead. Either way, the extension thread reports back to
`RunQueueExtender.releaseThread()`, which frees the thread ID for the next
borrower and records how many slices the task actually got.

## Observability

`Observer` is a small set of event hooks - `addTask`, `switchTask`,
`dequeueTask`, `enqueueTask`, `rebalance`, `workRequestRejected`,
`addToExtensionThread`, and the three extension-pool rejection reasons -
called from exactly the places already described above:
`TaskManager.addTaskEntity` calls `addTask`, `ThreadMQ` calls `dequeueTask`
when an entity stops being runnable, `acceptWorkMigration` calls
`rebalance`, `requestMoreWork` calls `workRequestRejected` when nobody has
spare work to give, and so on. None of these hooks feed back into any
scheduling decision - they're purely observational, which is what lets
`Observer.DUMMY` (a no-op implementation) be the default with zero
behavioral difference from having no observer at all.

`BasicStatsObserver` is the one real implementation: per-thread counters
(total tasks seen, currently runnable, peak runnable, number of times
scheduled, migrations in, rejected work requests) plus a few pool-wide
counters (current/peak active tasks, and the three extension-pool
rejection reasons, each counted independently so it's possible to tell
why the extension pool is being avoided - too busy, high load average, or
simply out of free threads). Because it only ever reacts to whatever it's
handed, `BasicStatsObserverTest` exercises it entirely on its own, with
hand-built fake task handles standing in for real `TaskEntity` objects - no
`TaskManager` needed at all.

## Testing approach

The test suite is layered to match how testable each piece actually is:

- Fully isolated unit tests (`LoadManagerTest`, `DiffQueueTest`,
  `BasicStatsObserverTest`, `DeferredTaskQueueTest`) for the components with
  no dependency on the `Entity`/`TaskManager` hierarchy.
- Focused, mechanism-level tests (`EntityStateMachineTest`,
  `ThreadMQTest`) that build a real `TaskManager` but only to isolate one
  specific mechanism - the state machine, or the staging/request queue
  handoff - rather than a full scheduling scenario. These are the tests to
  read first if a specific piece (not the overall fairness behavior) is
  unclear.
- Deterministic, hand-driven scheduling tests (`CfsSchedulerTest`,
  `RoundRobinSchedulerTest`) that exercise the full `TaskManager`/`ThreadMQ`
  machinery, but always through a manually-stepped virtual clock rather than
  real timing - fairness ratios, priority ordering, blocking/re-enqueue, load
  balancing, RPC-offload, and the extension pool are all covered this way.
- One real-concurrency test per scheduler, using actual background
  threads, asserting the properties that can't be faked by determinism -
  that every task completes exactly once, with no lost or duplicated work
  under genuine concurrent access. These tasks run real code, so the tests
  also check that it executed exactly once per tick and on the scheduler's
  own worker threads.
- Real-work tests in both scheduler test classes: tasks that append to a
  shared log, sum numbers, or bump counters, so the execution order
  (round-robin turn order, strict priority) and the results (correct sums,
  a 2:1 ratio of real work for weights 2 and 1) come from code that actually
  ran rather than from counters alone.

Every quantitative fairness claim in this codebase (weight ratios, the
groupPenalty correction, min_vruntime behavior) was checked by actually
running the code and reading the numbers back rather than derived on paper
and assumed correct - including the groupPenalty formula and the
multi-member asymmetry described above, both of which only became apparent
once measured.

## Build and test

```
mvn test
```

No external services or monorepo parent needed - `pom.xml` here is fully
standalone (junit only).

## Further reading

- [BFS vs. CFS](https://www.cs.unm.edu/~eschulte/data/bfs-v-cfs_groves-knockel-schulte.pdf)
  (Groves, Knockel, Schulte) - a good source for the performance metrics
  (CPU utilization, throughput, turnaround time, waiting time) worth
  tracking when comparing schedulers like the two in this project.

- https://github.com/dremio/dremio-oss