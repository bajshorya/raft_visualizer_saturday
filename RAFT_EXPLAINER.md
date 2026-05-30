# Raft Consensus — Full Code Explainer

This document explains everything in `src/main.rs`: what Raft is, why the data
structures are shaped the way they are, how every handler works step by step,
and what the correctness rules mean in practice.

---

## Table of Contents

1. [What is Raft?](#1-what-is-raft)
2. [The Core Problem](#2-the-core-problem)
3. [Key Concepts Before Reading the Code](#3-key-concepts-before-reading-the-code)
4. [Data Structures](#4-data-structures)
5. [The Message System](#5-the-message-system)
6. [Helper Functions](#6-helper-functions)
7. [The Four Handlers](#7-the-four-handlers)
8. [Full Flow Walkthrough](#8-full-flow-walkthrough)
9. [Raft Safety Rules in This Code](#9-raft-safety-rules-in-this-code)
10. [Rust-Specific Design Decisions](#10-rust-specific-design-decisions)
11. [What Is Not Yet Implemented](#11-what-is-not-yet-implemented)

---

## 1. What is Raft?

Raft is a **consensus algorithm**. It lets a cluster of machines agree on a
single sequence of values (a log) even when some machines crash or messages
are delayed.

A classic use case: you have 5 database servers. A client writes a record.
Raft ensures all 5 servers eventually store that record in the same position
in their log, and that no two servers ever disagree about what was committed —
even if 2 servers crash mid-write.

Raft was designed by Diego Ongaro and John Ousterhout to be **understandable**.
Where Paxos is notoriously hard to reason about, Raft decomposes the problem
into three mostly-independent subproblems:

- **Leader election** — one node is elected the single authority.
- **Log replication** — the leader takes client commands and replicates them.
- **Safety** — only a leader with an up-to-date log can be elected; only
  entries acknowledged by a majority can ever be committed.

---

## 2. The Core Problem

Imagine 5 nodes. The client sends a command `"set x 42"`.

```
Client ──► Node 1  (Leader)
           Node 2  (Follower)
           Node 3  (Follower)
           Node 4  (Follower)
           Node 5  (Follower)  ← crashed
```

The leader appends `"set x 42"` to its own log, then asks every follower to
do the same. Once **a majority** (3 of 5) confirm, the entry is **committed**
— it can never be undone, even if the leader crashes right after.

The hard part: what if Node 1 crashes after telling Node 2 and Node 3 but
before telling Node 4 and Node 5? A new election must happen. The new leader
must not be Node 4 or Node 5 (their logs are behind). The algorithm must
guarantee that `"set x 42"` either stays in the log forever or disappears
entirely — never partially committed.

Raft solves this with three interlocking rules:
1. Only a node with an up-to-date log can win an election.
2. An entry is only committed once a majority confirms it.
3. A leader can only commit entries it wrote itself in the current term
   (not leftover entries from a previous leader).

---

## 3. Key Concepts Before Reading the Code

### Term

A **term** is a monotonically increasing integer. Think of it as an epoch.
Every election starts a new term. Every message carries a term number.

```
Term 1: Node A is leader
Term 2: Node A crashed, election → Node B is leader
Term 3: Node B crashed, election → Node C is leader
```

If a node sees a message with a term higher than its own, it knows it missed
an election. It immediately reverts to Follower and updates its term. This is
the most important rule in the entire algorithm.

### Role

Every node is always in exactly one of three roles:

- **Follower** — passive. Responds to RPCs. If it doesn't hear from a leader
  for a while, it starts an election.
- **Candidate** — running for leader. Sends `RequestVote` to all peers,
  collects votes. If it wins a majority, it becomes Leader.
- **Leader** — the single authority. Accepts client commands, replicates them
  to followers, decides what is committed.

### Log

The log is an ordered list of entries. Each entry has:
- An **index** (position in the log, 1-based; index 0 is a sentinel).
- A **term** (which leader term the entry was created in).
- A **command** (the state machine instruction, e.g. `"set x 42"`).

The entire point of Raft is to ensure every non-crashed node ends up with
the same log in the same order.

### Committed vs. Applied

- **Committed**: a majority of nodes have the entry in their log. It can
  never be overwritten. The leader advances `commit_index` to mark this.
- **Applied**: the entry has been handed to the state machine (executed).
  `last_applied` tracks this. Applied always chases committed.

These are separate because applying is not instant — you might need to notify
a database, a WebSocket client, etc.

---

## 4. Data Structures

### `NodeId`

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct NodeId(u64);
```

A **newtype** around `u64`. The wrapper exists purely for type safety — you
can never accidentally pass a raw count or index where a node ID is expected.

The derives are required for practical use:
- `Copy` — no need to `.clone()` when passing around.
- `PartialEq + Eq` — needed for `==` comparisons (e.g. checking `voted_for`).
- `Hash` — required to use `NodeId` as a key in `HashMap` and `HashSet`.

---

### `Role`

```rust
enum Role {
    Follower,
    Candidate { votes_received: HashSet<NodeId> },
    Leader {
        next_index:  HashMap<NodeId, usize>,
        match_index: HashMap<NodeId, usize>,
    },
}
```

This is one of the key design choices. Leader-only state lives **inside** the
`Leader` variant. You cannot accidentally access `next_index` from a Follower
— the compiler makes it impossible. No runtime `if self.is_leader()` guards.

**`votes_received`** (Candidate only):
The set of peers that have granted us a vote this election. We seed it with
our own `NodeId` when the election starts (we always vote for ourselves).
When `votes_received.len() > majority_threshold()`, we've won.

**`next_index`** (Leader only):
A map from each peer's `NodeId` to the log index we *plan* to send next.
Starts optimistically at `last_log_index + 1` (assume peers are caught up).
Backs down by 1 each time a peer rejects `AppendEntries`.

**`match_index`** (Leader only):
A map from each peer's `NodeId` to the highest log index we *know* has been
replicated there (confirmed by a successful `AppendEntriesResponse`).
Starts at 0 (nothing confirmed). Used to decide when we can advance
`commit_index`.

```
next_index  = optimistic guess (may be ahead of what peer actually has)
match_index = confirmed truth  (always conservative)

next_index[peer] is always >= match_index[peer] + 1
```

---

### `LogEntry`

```rust
#[derive(Clone, Debug)]
struct LogEntry {
    index:   usize,
    term:    u64,
    command: String,
}
```

One slot in the replicated log. `Clone` is required because we copy slices of
the log into `AppendEntries` RPCs to send to peers.

**The sentinel at index 0**: the log is always initialised with one empty
entry at `log[0]` with `term = 0`. This exists so that `prev_log_index = 0`
always has a valid entry to compare against — the very first `AppendEntries`
(for `log[1]`) uses `prev_log_index = 0, prev_log_term = 0`, which matches
the sentinel. Without it, you'd need special-casing everywhere for "the entry
before the first real entry".

---

### `RaftNode`

```rust
struct RaftNode {
    id:            NodeId,
    peers:         Vec<NodeId>,
    role:          Role,

    // Persistent (survive crash)
    current_term:  u64,
    voted_for:     Option<NodeId>,
    log:           Vec<LogEntry>,

    // Volatile (reconstructible)
    commit_index:  usize,
    last_applied:  usize,

    inbox:         mpsc::Receiver<Message>,
}
```

The entire state of one Raft node.

**Persistent state** must be written to stable storage before replying to any
RPC. If the node crashes and reboots, it reads these back. If it didn't
persist them and replied first, it could violate safety on restart.

- `current_term` — forgetting this would allow a restarted node to accept old
  RPCs it should reject.
- `voted_for` — forgetting this could cause a node to vote twice in the same
  term (once before crash, once after), which could allow two leaders in one
  term.
- `log` — obviously must survive crash; this is the data.

**Volatile state** can safely be 0 on restart:

- `commit_index` — the leader will re-tell us via `AppendEntries` `leader_commit`.
- `last_applied` — we re-apply from index 1 up to `commit_index` on startup.

**`inbox`** — the receive-end of an `mpsc` channel. Every `Message` this node
receives (RPCs, responses, timeouts, client commands) arrives here. One
channel per node; one node per async task.

---

## 5. The Message System

```
┌──────────────────────────────────────────┐
│              Message enum                │
│                                          │
│  Rpc { from, payload: RpcMessage }       │  ← arriving RPC
│  RpcResponse(RpcResponse)                │  ← reply to our RPC
│  Timeout(TimeoutKind)                    │  ← timer fired
│  ClientCommand(String)                   │  ← client wants to write
└──────────────────────────────────────────┘
```

### `RpcMessage` — what we receive

```rust
enum RpcMessage {
    RequestVote    { term, candidate_id, last_log_index, last_log_term },
    AppendEntries  { term, leader_id, prev_log_index, prev_log_term,
                     entries, leader_commit },
}
```

`RequestVote`: sent by a Candidate to all peers asking for a vote.
- `last_log_index / last_log_term`: proves the candidate's log is up-to-date.

`AppendEntries`: sent by the Leader to all followers.
- Also serves as a heartbeat when `entries = []`.
- `prev_log_index / prev_log_term`: the consistency check anchor.
- `leader_commit`: the leader's current `commit_index`, so followers can
  advance theirs.

### `RpcResponse` — replies we receive

```rust
enum RpcResponse {
    RequestVoteResponse    { from, term, vote_granted },
    AppendEntriesResponse  { from, term, success, match_index },
}
```

Both responses carry `term` so we can detect if the responder is in a higher
term (which means we need to step down).

`AppendEntriesResponse.match_index`: on success, this is the highest index
the follower now has. The leader uses this to update its `match_index` map
and advance `commit_index`.

### `OutboundMsg` — what handlers return

```rust
enum OutboundMsg {
    Rpc      { to: NodeId, payload: RpcMessage  },
    Response { to: NodeId, payload: RpcResponse },
}
```

Handlers never send on a channel directly. They return a `Vec<OutboundMsg>`.
The event loop (not yet implemented) dispatches these. This design makes
handlers completely pure and testable — you can call any handler with
fabricated inputs and assert on the returned `Vec`.

---

## 6. Helper Functions

### `last_log_index` and `last_log_term`

```rust
fn last_log_index(&self) -> usize { self.log.len() - 1 }
fn last_log_term(&self)  -> u64   { self.log.last().map(|e| e.term).unwrap_or(0) }
```

Because the sentinel is always at index 0, `log.len() - 1` never underflows.
These are called frequently in election and replication logic.

---

### `is_log_up_to_date`

```rust
fn is_log_up_to_date(&self, candidate_last_index: usize, candidate_last_term: u64) -> bool {
    let my_term = self.last_log_term();
    if candidate_last_term != my_term {
        candidate_last_term > my_term
    } else {
        candidate_last_index >= self.last_log_index()
    }
}
```

Implements §5.4.1's "at least as up-to-date" comparison. Used in
`RequestVote` handling to decide whether to grant a vote.

**Rule**: compare the *last entry's term* first. If they differ, the higher
term wins. If they're equal, the *longer log* wins.

**Why terms beat length**: Suppose Node A has log `[1, 1, 2]` (terms) and
Node B has `[1, 1, 1, 1]`. Node A wins despite being shorter, because its
last entry is in term 2. A term-2 entry means a term-2 leader committed it to
Node A's log. Node B's log only has term-1 entries — if there was a term-2
leader, it didn't replicate to Node B. We must not elect Node B or its older
log could overwrite Node A's term-2 data.

---

### `majority_threshold`

```rust
fn majority_threshold(&self) -> usize {
    (self.peers.len() + 1) / 2
}
```

Integer floor of half the cluster. Majority = any count **strictly greater**
than this threshold.

```
Cluster size  │  threshold  │  votes needed to win
──────────────┼─────────────┼─────────────────────
3 nodes       │  1          │  > 1  →  2 votes
5 nodes       │  2          │  > 2  →  3 votes
7 nodes       │  3          │  > 3  →  4 votes
```

This means Raft can tolerate `floor((n-1)/2)` simultaneous failures:
- 3-node cluster: tolerates 1 failure (2 remaining = majority)
- 5-node cluster: tolerates 2 failures (3 remaining = majority)

---

### `step_down_if_stale`

```rust
fn step_down_if_stale(&mut self, incoming_term: u64) {
    if incoming_term > self.current_term {
        self.current_term = incoming_term;
        self.role = Role::Follower;
        self.voted_for = None;
    }
}
```

**The highest-priority rule in Raft.** Called at the very top of every
handler, before any other logic.

If any message carries a term higher than ours:
1. Update our `current_term` to that higher value.
2. Revert to `Follower` (no matter what role we were in — even Leader).
3. Clear `voted_for` so we can vote in the new term.

This is what ensures there is never more than one leader per term. A leader
in term 3 that receives a message from term 4 immediately stops being a
leader and becomes a follower.

---

### `become_leader`

```rust
fn become_leader(&mut self) -> Vec<OutboundMsg> {
    let last = self.last_log_index();
    self.role = Role::Leader {
        next_index:  self.peers.iter().map(|&p| (p, last + 1)).collect(),
        match_index: self.peers.iter().map(|&p| (p, 0)).collect(),
    };
    self.send_heartbeats()
}
```

Called when a Candidate has accumulated enough votes.

Sets `next_index[peer] = last + 1` for everyone: the **optimistic
initialisation**. The leader assumes all followers are caught up. If they're
not, the next `AppendEntriesResponse` will tell us (via `success = false`)
and we'll back `next_index` down.

Sets `match_index[peer] = 0` for everyone: we haven't confirmed anything yet.

Then immediately calls `send_heartbeats()` to assert leadership before
any follower's election timer fires.

---

### `send_heartbeats`

```rust
fn send_heartbeats(&self) -> Vec<OutboundMsg> { ... }
```

Builds one `AppendEntries` RPC per peer. The entries slice is everything in
our log from `next_index[peer]` onward. If the peer is fully caught up, this
slice is empty — making it a pure heartbeat.

```
log:    [0]sentinel [1]term1 [2]term1 [3]term2
                                          ↑
next_index[peer2] = 3    →  entries = [log[3]]   (peer2 is one behind)
next_index[peer3] = 4    →  entries = []          (peer3 is caught up)
```

**Why clone `next_index`?** Rust's borrow checker: the `let Role::Leader { ref next_index, .. }` borrow keeps `self.role` borrowed immutably. Then inside
the `.map()` closure we need to access `self.log` (also part of `self`). Even
though these are different fields, the compiler sees them both as borrows of
`self` and rejects the code. Cloning `next_index` into a local `HashMap`
ends the borrow on `self.role` before the closure runs.

---

### `try_advance_commit_index`

```rust
fn try_advance_commit_index(&mut self) { ... }
```

Called after every successful `AppendEntriesResponse`. Scans the log
downward to find the highest index `N` that satisfies all three conditions:

```
1. N > commit_index          (not already committed)
2. majority of nodes have match_index >= N   (replicated on enough nodes)
3. log[N].term == current_term               (Figure 8 safety)
```

**Why scan downward?** We want the highest possible `N`. Scanning from the
top and breaking at the first success gives us that in O(log) iterations in
practice (most of the time we only advance by 1).

**The snapshot trick**: we collect `match_index.values()` into an owned
`Vec<usize>` before the loop. This ends the immutable borrow on `self.role`
(needed for the `Role::Leader` match). The loop then freely borrows `self.log`
and `self.commit_index`. Without the snapshot, Rust would reject the code for
holding an active borrow on `self.role` while also accessing other fields of
`self`.

---

## 7. The Four Handlers

All handlers have the same signature contract:
```rust
fn handle_*(mut &self, ...) -> Vec<OutboundMsg>
```
They read and mutate `self` (the node's state) and return a list of messages
for the event loop to dispatch. No I/O, no async, no channels.

---

### Handler 1: `handle_rpc`

Processes incoming RPCs from other nodes.

#### RequestVote

```
Candidate Node B  ──RequestVote(term=3)──►  Node A
                                              │
                  ◄──VoteGranted/Denied───────┘
```

Decision tree:

```
receive RequestVote { term, candidate_id, last_log_index, last_log_term }
│
├─ step_down_if_stale(term)         // update if behind
│
├─ if term < current_term
│    └─ REJECT (candidate is stale)
│
├─ can_vote = voted_for is None OR voted_for == candidate_id
├─ log_ok  = is_log_up_to_date(last_log_index, last_log_term)
│
├─ if can_vote AND log_ok
│    ├─ voted_for = candidate_id
│    └─ GRANT vote
│
└─ else DENY vote
```

The `voted_for == candidate_id` check handles **retransmitted** RequestVotes.
If the candidate's first RPC was lost and it retries, we should grant again —
we already made the decision. This makes the grant idempotent.

After granting, the event loop (future step) must reset the election timer.
If we don't reset it, we might start our own election right after helping
someone else start one.

#### AppendEntries

```
Leader  ──AppendEntries(term=3, prevIdx=2, prevTerm=2, entries=[3])──►  Follower
        ◄──AppendEntriesResponse(success=true, matchIndex=3)───────────
```

Decision tree:

```
receive AppendEntries { term, prev_log_index, prev_log_term, entries, leader_commit }
│
├─ step_down_if_stale(term)
│
├─ if term < current_term
│    └─ REJECT (stale leader)
│
├─ if role == Candidate
│    └─ step down to Follower (someone won the election)
│
├─ if log[prev_log_index].term != prev_log_term
│    └─ REJECT (log is inconsistent at this position)
│
├─ log.truncate(prev_log_index + 1)   // delete conflicting entries
├─ log.extend(entries)                // append new entries
│
├─ if leader_commit > commit_index
│    └─ commit_index = min(leader_commit, new_last_index)
│
└─ SUCCESS
```

**The consistency check** is the core of log replication safety. Before
appending anything, the follower checks: "do I have an entry at
`prev_log_index` with exactly `prev_log_term`?" If not, it means our log
diverges somewhere. We reject, and the leader will back up `next_index` and
retry from an earlier position.

**Truncation is idempotent**: if the leader re-sends entries we already have
(e.g. after a network hiccup), we truncate back to `prev_log_index` and
re-extend with the same entries. The log ends up identical.

**Commit index update**: the leader tells us its current `commit_index` via
`leader_commit`. We advance ours — but clamped to `new_last_index`, because
the leader might have committed entries we haven't received yet. We can only
mark as committed what we actually have.

---

### Handler 2: `handle_response`

Processes replies to RPCs we sent.

#### RequestVoteResponse

```
receive RequestVoteResponse { from, term, vote_granted }
│
├─ step_down_if_stale(term)
│
├─ if !vote_granted OR role != Candidate
│    └─ ignore (stale response, or we've already won/lost)
│
├─ votes_received.insert(from)
│
├─ if votes_received.len() > majority_threshold()
│    └─ become_leader()          // won the election!
│
└─ else wait for more votes
```

**The borrow checker dance**: we need to insert `from` into `votes_received`
(which lives inside `self.role`), then call `become_leader()` (which also
mutates `self.role`). These are two mutable borrows of `self.role` and Rust
won't allow them to overlap.

The solution: put the insert and the count-check inside an explicit block `{
... }`. The block ends, the borrow of `self.role` ends, and *then* we call
`become_leader()`. The `won: bool` local variable carries the result out of
the block.

```rust
let won = {
    let Role::Candidate { ref mut votes_received } = self.role else { ... };
    votes_received.insert(from);
    votes_received.len() > threshold
}; // borrow of self.role ends here

if won { return self.become_leader(); }
```

#### AppendEntriesResponse

```
receive AppendEntriesResponse { from, term, success, match_index: peer_match }
│
├─ step_down_if_stale(term)
│
├─ if success
│    ├─ match_index[from] = peer_match
│    ├─ next_index[from]  = peer_match + 1
│    └─ try_advance_commit_index()
│
└─ if failure
     └─ next_index[from] -= 1   (retry with earlier prefix next heartbeat)
```

**On success**: the follower's log now matches ours up through `peer_match`.
We update our tracking tables and check if we can advance `commit_index`.

**On failure**: the follower's log diverges somewhere before our current guess.
We back up `next_index` by 1 and try again on the next heartbeat with a longer
prefix (`prev_log_index` will be one step earlier). This is O(n) in the worst
case (all entries wrong). An optimisation described in the paper (not yet
implemented) can make it O(1) by encoding which term conflicted in the response.

**Another borrow checker split**: the `if let Role::Leader { ... }` block for
updating the tables must end before calling `try_advance_commit_index`. Both
take `&mut self`, so they can't overlap.

---

### Handler 3: `handle_timeout`

#### Election timeout

```
┌──────────────────────────────────────────────────┐
│  No heartbeat received within [150ms, 300ms]     │
│  (random range reduces split-vote probability)   │
└──────────────────────────────────────────────────┘
         │
         ▼
current_term += 1                // new term starts NOW
voted_for = Some(self.id)        // vote for ourselves
role = Candidate { votes_received: {self.id} }

for each peer:
    send RequestVote {
        term           = current_term,
        candidate_id   = self.id,
        last_log_index = ...,
        last_log_term  = ...,
    }
```

Why increment term **before** sending? Because the `RequestVote` RPC carries
the term. If peers see a term equal to their own, they've already voted this
term (or are themselves running). By incrementing first, we signal "I am
starting a brand new election in a term nobody has seen yet".

Why vote for yourself immediately? Raft requires it — a candidate always
votes for itself. This is implicit in `voted_for = Some(self.id)` and seeding
`votes_received` with `self.id`.

#### Heartbeat timeout

```
if role != Leader → return (safety guard; timer should have been cancelled)

send_heartbeats() for all peers
```

The heartbeat interval must be **less than** the minimum election timeout.
If heartbeats arrive reliably, followers never hit their election timeout.

```
Heartbeat interval:  ~50ms
Election timeout:    150ms – 300ms (random per node)

If the leader is alive: followers reset their election timer every 50ms,
so they never reach 150ms.
```

Heartbeats also carry log entries: if a follower is behind, the entries slice
in `AppendEntries` won't be empty. Log replication and heartbeat are the same
RPC — no separate mechanism needed.

---

### Handler 4: `handle_command`

```
if role != Leader → return (can't accept; should redirect to leader)

log.push(LogEntry {
    index:   log.len(),     // next slot after sentinel
    term:    current_term,
    command: cmd,
})

return []   // no immediate messages; replication via next heartbeat
```

The entry is **uncommitted** when this returns. It sits in the leader's log,
waiting. On the next heartbeat fire, `send_heartbeats` will pick it up (it's
now part of `self.log[next_index[peer]..]`) and send it to all followers.

When enough followers confirm receipt (`AppendEntriesResponse success=true`),
`try_advance_commit_index` will advance `commit_index` past this entry. At
that point, the entry is committed and will be applied to the state machine.

---

## 8. Full Flow Walkthrough

Here's the complete lifecycle from startup to a committed command in a 3-node
cluster (nodes A, B, C).

### Step 1: Startup (all nodes are Followers)

```
A: role=Follower, term=0, log=[sentinel]
B: role=Follower, term=0, log=[sentinel]
C: role=Follower, term=0, log=[sentinel]
```

All three nodes start election timers with random durations.

### Step 2: A's election timer fires first

```
A: handle_timeout(Election)
   → current_term = 1
   → voted_for = Some(A)
   → role = Candidate { votes_received: {A} }
   → send RequestVote(term=1) to B and C
```

### Step 3: B and C receive RequestVote

```
B: handle_rpc(RequestVote { term=1, candidate=A, ... })
   → step_down_if_stale(1): no-op (terms equal)
   → term not stale
   → voted_for=None, log is up-to-date → grant
   → voted_for = Some(A)
   → send RequestVoteResponse(granted=true) to A

C: (same as B)
```

### Step 4: A receives votes

```
A: handle_response(RequestVoteResponse { from=B, granted=true })
   → votes_received = {A, B}
   → len=2 > threshold=1 → WON

A: become_leader()
   → role = Leader { next_index: {B:1, C:1}, match_index: {B:0, C:0} }
   → send_heartbeats():
       AppendEntries(term=1, prevIdx=0, prevTerm=0, entries=[], commit=0) → B
       AppendEntries(term=1, prevIdx=0, prevTerm=0, entries=[], commit=0) → C
```

### Step 5: Client sends a command

```
Client → A: handle_command("set x 42")
   → log = [sentinel, LogEntry{index=1, term=1, command="set x 42"}]
   → return []   (no outbound — waits for heartbeat)
```

### Step 6: Next heartbeat fires on A

```
A: handle_timeout(Heartbeat)
   → send_heartbeats():
       next_index[B] = 1, so entries = log[1..] = [LogEntry{1, term=1, "set x 42"}]
       AppendEntries(term=1, prevIdx=0, prevTerm=0, entries=[{1,1,"set x 42"}], commit=0) → B
       AppendEntries(term=1, prevIdx=0, prevTerm=0, entries=[{1,1,"set x 42"}], commit=0) → C
```

### Step 7: B and C append the entry

```
B: handle_rpc(AppendEntries { prevIdx=0, prevTerm=0, entries=[...] })
   → log[0].term == 0 ✓  (sentinel matches)
   → truncate to [sentinel], extend with [LogEntry{1,1,"set x 42"}]
   → log = [sentinel, LogEntry{1,1,"set x 42"}]
   → commit_index stays 0 (leader_commit=0)
   → send AppendEntriesResponse(success=true, match_index=1)

C: (same)
```

### Step 8: A receives confirmations and commits

```
A: handle_response(AppendEntriesResponse { from=B, success=true, match_index=1 })
   → match_index[B] = 1, next_index[B] = 2
   → try_advance_commit_index():
       N=1: log[1].term=1 == current_term=1 ✓
            replicas = 1 (self) + 1 (B has match_index>=1) = 2
            2 > threshold=1 → commit_index = 1
   → "set x 42" is now COMMITTED

A: handle_response(AppendEntriesResponse { from=C, success=true, match_index=1 })
   → match_index[C] = 1, next_index[C] = 2
   → try_advance_commit_index(): N=1 already committed, no change
```

### Step 9: A broadcasts the new commit_index

On the next heartbeat, A sends `leader_commit=1` to B and C. They advance
their own `commit_index` to 1. All three nodes then chase `last_applied` up
to `commit_index=1` and apply `"set x 42"` to their state machines.

```
Final state (all 3 nodes):
   log          = [sentinel, {1, term=1, "set x 42"}]
   commit_index = 1
   last_applied = 1    (after applying)
```

---

## 9. Raft Safety Rules in This Code

### Rule 1: Term monotonicity (`step_down_if_stale`)

Called at the **top of every single handler** before any logic runs. If any
incoming message has a higher term, we immediately update and step down.

This guarantees: **in any given term, at most one leader exists.**

Without this, you could have two leaders in the same term issuing conflicting
`AppendEntries` to followers, corrupting the log.

---

### Rule 2: One vote per term (`voted_for`)

Each node has one `voted_for` slot. Once set in a term, it can only be set
to the same value again (idempotent re-vote). It's cleared only when `term`
advances.

This guarantees: **no node votes for two candidates in the same term.**

Combined with Rule 1, this guarantees: **at most one candidate can win a
majority in any term.**

---

### Rule 3: Log up-to-date check (`is_log_up_to_date`)

Before granting a vote, we verify the candidate's log is at least as
up-to-date as ours.

This guarantees: **any elected leader has all committed entries.**

Without this, a stale node with a short log could win an election, become
leader, and start sending `AppendEntries` that overwrite entries that were
already committed on a majority of the old cluster.

---

### Rule 4: Majority required to commit (`try_advance_commit_index`)

`commit_index` only advances when `match_index >= N` on more than half the
cluster. Not just 1 node, not all nodes — a **majority**.

This guarantees: **any committed entry survives any failure that leaves a
majority alive.**

If we committed after just 1 confirmation, a 2-node crash could wipe out the
only copy.

---

### Rule 5: Only commit current-term entries (Figure 8)

```rust
if self.log[n].term != self.current_term { continue; }
```

This is the subtlest rule. A leader may have entries in its log from *previous
terms* (entries it inherited but never committed). It must NOT commit those
entries by replica-counting alone, even if they appear on a majority.

**Why?** The Figure 8 scenario from the paper:

```
Step 1: Node A (leader, term=2) replicates entry [term=2] to B only, then crashes.
Step 2: Node C (term=3) wins election. C doesn't have term=2's entry.
         C starts replicating its own entries.
Step 3: Node A restarts. Now A is the leader again (term=4). A still has the
         term=2 entry and replicates it to a majority.
Step 4: If A could commit that term=2 entry now (it's on a majority), and then
         A crashes — C could win another election (C has a longer term=3 entry).
         C's AppendEntries would OVERWRITE the supposedly committed term=2 entry.
         SAFETY VIOLATION.
```

The fix: A must only commit by counting once it has written a **current-term**
entry to a majority. When that current-term entry is committed, all older
entries before it are implicitly committed too (they're in the same log prefix).

In our code: `try_advance_commit_index` skips any `N` where
`log[N].term != current_term`. Old entries will be committed automatically
when the first new-term entry passes the commit threshold.

---

## 10. Rust-Specific Design Decisions

### Why `Role` carries its state in enum variants

Standard OOP would use a struct field `is_leader: bool` and separate maps for
`next_index`. In Rust, putting the data inside the enum variant means you
**pattern-match to access it**. The compiler proves at the call site that
`next_index` is only touched when the node is a Leader:

```rust
// Compiler error if you try this outside a Leader match:
self.role.next_index  // ← doesn't compile; Role has no field next_index

// Correct — only accessible via match:
if let Role::Leader { ref next_index, .. } = self.role { ... }
```

### Why handlers return `Vec<OutboundMsg>` instead of sending on channels

Sending on a channel is a side effect. Side effects make functions hard to
test. By returning the messages, you can unit-test every handler by simply
calling it and inspecting the `Vec`. No mocking, no channels, no async
runtime needed in tests.

```rust
// A future unit test looks like:
let mut node = make_test_node();
let msgs = node.handle_timeout(TimeoutKind::Election);
assert_eq!(msgs.len(), 4);  // 4 peers got RequestVote
```

### The borrow-checker split pattern

Several places in the code do:

```rust
let data: OwnedType = {
    let SomeVariant { ref field, .. } = self.some_enum else { return; };
    field.clone_or_compute()
}; // borrow ends here

self.some_method_that_mutates(); // now safe
```

This is a recurring pattern when you need to (a) read something from a
field that borrows `self`, and (b) then call a `&mut self` method. The
explicit block ends the borrow, and the owned value carries the data forward.

---

## 11. What Is Not Yet Implemented

This is **Build Step 2** of the CLAUDE.md plan. What's done:

- [x] All data structures (`NodeId`, `Role`, `LogEntry`, `RaftNode`, all enums)
- [x] All four handler functions with full Raft logic
- [x] Helper functions

What comes next:

**Step 3 — Unit tests**
Test the handlers directly: simulate `RequestVote` sequences, verify that
`voted_for` is set, that stale term votes are rejected, that `commit_index`
advances correctly after a majority confirms, etc. No async runtime needed.

**Step 4 — Async event loop + timers**
The `run()` loop that reads from `inbox`, calls `handle_message()`, and
dispatches the returned `OutboundMsg` values. Two timer tasks per node:
- Election timer: `tokio::time::sleep(random_duration)`, then send
  `Message::Timeout(Election)` into the inbox.
- Heartbeat timer: fixed interval, same idea.
- Reset = `AbortHandle::abort()` the old task + spawn a new one.

**Step 5 — Integration test**
Spawn 5 nodes, wire them together with `mpsc` channels, submit commands,
assert that `commit_index` advances consistently across all nodes.

**Step 6 — WebSocket bridge**
An `emit()` function that sends `StateEvent` JSON to a broadcast channel.
Called from the event loop after every state change.

**Step 7 — Next.js frontend**
Connects to the WebSocket, renders node circles with role/term/log state,
animated arrows, controls for crashing/restarting nodes and network partitions.

---

*Reference: Raft paper — https://raft.github.io/raft.pdf*
*Sections that matter most: §3 (basics), §5 (leader election + log replication), §7 (safety proof)*
