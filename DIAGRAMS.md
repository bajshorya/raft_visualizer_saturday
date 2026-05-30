# Raft Implementation — Diagrams & Visual Reference

Every process, data structure, workflow, and interaction in the codebase
shown as ASCII diagrams. Read top-to-bottom for increasing depth.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Module Dependency Graph](#2-module-dependency-graph)
3. [Data Structure Map](#3-data-structure-map)
4. [Raft Role State Machine](#4-raft-role-state-machine)
5. [Cluster Wiring](#5-cluster-wiring)
6. [NodeRunner Internals](#6-noderunner-internals)
7. [The Event Loop](#7-the-event-loop)
8. [Message Type Taxonomy](#8-message-type-taxonomy)
9. [Election Flow](#9-election-flow)
10. [Log Replication Flow](#10-log-replication-flow)
11. [Commit Index Advancement](#11-commit-index-advancement)
12. [Handler Decision Trees](#12-handler-decision-trees)
13. [Timer Architecture](#13-timer-architecture)
14. [Figure 8 Safety](#14-figure-8-safety)
15. [Test Architecture](#15-test-architecture)
16. [Full Message Lifecycle](#16-full-message-lifecycle)

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Raft Cluster                             │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │  Node 1  │  │  Node 2  │  │  Node 3  │  │  Node 4  │  │  Node 5  │ │
│  │Follower  │  │Follower  │  │ LEADER   │  │Follower  │  │Follower  │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘ │
│       │              │              │              │              │      │
│       └──────────────┴──────────────┴──────────────┴─────────────┘      │
│                          mpsc channels (in-process)                     │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │ watch::Receiver<usize> per node
                                   │ (commit_index observable)
                   ┌───────────────┴───────────────┐
                   │        [Step 6 — TODO]         │
                   │    WebSocket Bridge (Axum)     │
                   │    broadcast::Sender<StateEvent│
                   └───────────────┬───────────────┘
                                   │ JSON over WebSocket  /ws
                   ┌───────────────┴───────────────┐
                   │      [Step 7 — TODO]           │
                   │    Next.js Frontend            │
                   │  node circles · log panel      │
                   │  election timers · controls    │
                   └───────────────────────────────┘
```

---

## 2. Module Dependency Graph

```
                    ┌─────────────┐
                    │   lib.rs    │  crate root — declares pub mods
                    │  (pub mods) │  enables tests/ to import backend::*
                    └──────┬──────┘
           ┌───────────────┼───────────────────┐
           ▼               ▼                   ▼
    ┌────────────┐  ┌────────────┐     ┌─────────────┐
    │  state.rs  │  │ message.rs │     │  handlers.rs│
    │            │◄─┤            │     │             │
    │  NodeId    │  │  Message   │     │ impl RaftNode│
    │  Role      │─►│  RpcMessage│     │ 4 handlers  │
    │  LogEntry  │  │  RpcResp   │     │ 35 unit tests│
    │  RaftNode  │  │  Outbound  │     └──────┬──────┘
    └────────────┘  └────────────┘            │
           ▲               ▲                  │ uses both
           └───────────────┴──────────────────┘
                           │
                    ┌──────▼──────┐
                    │   node.rs   │
                    │             │
                    │ NodeRunner  │  async shell
                    │ spawn_      │  timers, dispatch
                    │  cluster()  │  watch channels
                    └──────┬──────┘
                           │ uses
                    ┌──────▼──────┐
                    │   main.rs   │
                    │             │
                    │ demo binary │
                    └─────────────┘

    tests/
    └── cluster_integration.rs
            uses backend::node::spawn_cluster
            uses backend::message::Message
```

**Key rule**: `state.rs` + `message.rs` + `handlers.rs` have zero Tokio
`async` usage — pure synchronous Rust. `node.rs` is the only async file.

---

## 3. Data Structure Map

```
RaftNode
├── id:            NodeId(u64)          ← newtype, never a raw number
├── peers:         Vec<NodeId>
│
├── role:          Role  ◄──────────────────────────────────────┐
│   ├── Follower                                                 │
│   ├── Candidate                                                │
│   │   └── votes_received: HashSet<NodeId>                      │
│   └── Leader                                                   │
│       ├── next_index:  HashMap<NodeId, usize>  ◄── only here   │
│       └── match_index: HashMap<NodeId, usize>  ◄── only here   │
│                                                                │
│   The compiler enforces Leader-only fields are unreachable     │
│   outside a `Role::Leader { .. }` pattern match.              │
│                                                                │
├── current_term:  u64          ┐                                │
├── voted_for:     Option<NodeId│ PERSISTENT                     │
├── log:           Vec<LogEntry>┘ (survives crash)               │
│   └── LogEntry                                                 │
│       ├── index:   usize                                       │
│       ├── term:    u64                                         │
│       └── command: String                                      │
│       [0] = sentinel (term=0, "")  ← never remove             │
│       [1] = first real entry                                   │
│       [N] = ...                                                │
│                                                                │
├── commit_index:  usize  ┐ VOLATILE                             │
├── last_applied:  usize  ┘ (safe to reset on crash)             │
│                                                                │
└── inbox: mpsc::Receiver<Message>                               │
                                                                 │
NodeRunner (wraps RaftNode)                                      │
├── node:           RaftNode  ───────────────────────────────────┘
├── self_tx:        mpsc::Sender<Message>     ← timer tasks write here
├── peers:          HashMap<NodeId, Sender>  ← dispatch writes here
├── commit_tx:      watch::Sender<usize>     ← publishes commit_index
├── election_timer: Option<AbortHandle>
└── heartbeat_timer:Option<AbortHandle>
```

---

## 4. Raft Role State Machine

```
                    ┌─────────────────────────────────┐
                    │          Any Message             │
                    │    with term > current_term      │
                    │  ──────────────────────────────  │
                    │  step_down_if_stale() fires      │
                    │  current_term = incoming_term    │
                    │  voted_for    = None             │
                    └──────────────┬──────────────────┘
                                   │ regardless of current role
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│           ┌────────────┐                                        │
│    start  │            │  election timeout fires                │
│  ────────►│  FOLLOWER  ├──────────────────────────────────────► │
│           │            │  [150–300ms random, no heartbeat]      │
│           └─────┬──────┘                                        │
│                 ▲                                               │
│                 │ valid AppendEntries (same/higher term)        │
│                 │ OR higher term seen in any message            │
│                 │                                               │
│           ┌─────┴──────────────────────────────────────┐       │
│           │                                             │       │
│           │           CANDIDATE                         │       │
│           │                                             │       │
│           │  On entry:                                  │       │
│           │    current_term += 1                        │       │
│           │    voted_for = self                         │       │
│           │    votes_received = {self}                  │       │
│           │    → broadcast RequestVote to all peers     │       │
│           │                                             │       │
│           └─────────────┬───────────────────────────────┘       │
│                         │                                       │
│           majority votes│granted                                │
│           (votes.len() >│ cluster/2)                            │
│                         ▼                                       │
│           ┌─────────────────────────────┐                       │
│           │                             │                       │
│           │           LEADER            │                       │
│           │                             │                       │
│           │  On entry:                  │                       │
│           │    next_index[peer]  = last+1                       │
│           │    match_index[peer] = 0    │                       │
│           │    → send heartbeats NOW    │                       │
│           │                             │                       │
│           │  Periodic (every 50ms):     │                       │
│           │    send AppendEntries to    │                       │
│           │    all peers                │                       │
│           │                             │                       │
│           └─────────────────────────────┘                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. Cluster Wiring

`spawn_cluster(5)` creates this in-process network:

```
         ┌──────────────────────────────────────────────────────┐
         │                  spawn_cluster(5)                    │
         │                                                      │
         │  For each node i:                                    │
         │    mpsc::channel(128) → (tx_i, rx_i)                 │
         │    watch::channel(0)  → (commit_tx_i, commit_rx_i)   │
         │    NodeRunner { node: RaftNode { inbox: rx_i }, ... } │
         │    tokio::spawn(runner.run())                         │
         │                                                      │
         │  Returns:                                            │
         │    senders[i]     = tx_i.clone()  (inject messages)  │
         │    commit_rxs[i]  = commit_rx_i   (observe commits)  │
         └──────────────────────────────────────────────────────┘

Node 1                Node 2                Node 3
┌─────────┐          ┌─────────┐           ┌─────────┐
│NodeRunner│         │NodeRunner│           │NodeRunner│
│         │──tx_2──►│         │           │         │
│         │──tx_3──►│         │──tx_1──►  │         │
│         │──tx_4──►│         │──tx_3──►  │         │
│         │──tx_5──►│         │──tx_4──►  │         │
│         │◄──tx_1──│         │──tx_5──►  │         │
│         │         │         │◄──tx_1──  │         │
└────┬────┘         └────┬────┘           └────┬────┘
     │                   │                     │
commit_rx_1          commit_rx_2           commit_rx_3
     │                   │                     │
     └───────────────────┴─────────────────────┘
            observed by tests / WebSocket bridge

          (Nodes 4 and 5 follow same pattern — omitted for clarity)
```

Each `NodeRunner.peers` map holds a clone of every *other* node's sender:
```
NodeRunner(1).peers = { 2 → tx_2, 3 → tx_3, 4 → tx_4, 5 → tx_5 }
NodeRunner(2).peers = { 1 → tx_1, 3 → tx_3, 4 → tx_4, 5 → tx_5 }
...
```

---

## 6. NodeRunner Internals

```
┌──────────────────────────────────────────────────────────────┐
│                       NodeRunner                             │
│                                                              │
│  ┌──────────────────────────────────────┐                    │
│  │              RaftNode                │                    │
│  │  id, peers, role, current_term,      │                    │
│  │  voted_for, log, commit_index,       │                    │
│  │  last_applied, inbox (Receiver)      │                    │
│  └──────────────────────────────────────┘                    │
│                                                              │
│  self_tx ────────────────────────────────────────┐           │
│  (Sender clone of own inbox)                     │           │
│                                               ┌──┴────────┐  │
│                                               │ election  │  │
│  election_timer: Option<AbortHandle> ◄────────│ timer task│  │
│                                               │ sleep(R)  │  │
│                                               │ tx.send(  │  │
│                                               │  Timeout( │  │
│                                               │  Election │  │
│                                               │ ))        │  │
│                                               └───────────┘  │
│                                               ┌───────────┐  │
│                                               │ heartbeat │  │
│  heartbeat_timer: Option<AbortHandle> ◄───────│ loop task │  │
│                                               │ loop {    │  │
│                                               │  sleep(50)│  │
│                                               │  tx.send( │  │
│                                               │  Heartbeat│  │
│                                               │ )}        │  │
│                                               └───────────┘  │
│                                                              │
│  commit_tx: watch::Sender<usize> ──► watch::Receiver (tests) │
│                                                              │
│  peers: HashMap<NodeId, Sender> ──► other nodes' inboxes     │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. The Event Loop

`NodeRunner::run()` — one iteration per message:

```
                   ┌─────────────────────────┐
                   │   inbox.recv().await     │
                   │   (blocks until msg)     │
                   └────────────┬────────────┘
                                │
                   ┌────────────▼────────────┐
                   │  snapshot before state  │
                   │  was_leader             │
                   │  voted_for_before       │
                   │  commit_before          │
                   └────────────┬────────────┘
                                │
                   ┌────────────▼────────────┐
                   │   handle_message(msg)   │  ← PURE, SYNC
                   │   &mut RaftNode         │    mutates node
                   │   → Vec<OutboundMsg>    │    returns messages
                   └────────────┬────────────┘
                                │
              ┌─────────────────┼──────────────────┐
              │                 │                  │
   ┌──────────▼──────┐ ┌────────▼────────┐ ┌───────▼──────────┐
   │  detect changes │ │   log & publish │ │ dispatch outbound │
   │                 │ │                 │ │                   │
   │ is_now_leader   │ │ role changed?   │ │ for m in outbound │
   │ vote_granted    │ │  → eprintln!    │ │  Rpc { to, pay } →│
   │ accepted_ae     │ │ commit advanced?│ │   peers[to].send  │
   └──────────┬──────┘ │  → eprintln!   │ │  Response { to } →│
              │        │  → commit_tx   │ │   peers[to].send  │
              │        │    .send(idx)  │ └───────────────────┘
              │        └───────────────┘
              │
   ┌──────────▼────────────────────────────────────────────────┐
   │                    Timer Management                       │
   │                                                           │
   │  (false → true)  became Leader?                           │
   │    cancel_election_timer()                                │
   │    start_heartbeat_timer()                                │
   │                                                           │
   │  (true → false)  stepped down?                            │
   │    cancel_heartbeat_timer()                               │
   │    reset_election_timer()                                 │
   │                                                           │
   │  vote granted OR AppendEntries accepted?                  │
   │    reset_election_timer()  ← suppress competing election  │
   └───────────────────────────────────────────────────────────┘
                                │
                   ┌────────────▼────────────┐
                   │   loop back to recv()   │
                   └─────────────────────────┘
```

---

## 8. Message Type Taxonomy

```
Message  (arrives in inbox)
├── Rpc { from: NodeId, payload: RpcMessage }
│   ├── RpcMessage::RequestVote
│   │   ├── term:           u64
│   │   ├── candidate_id:   NodeId
│   │   ├── last_log_index: usize
│   │   └── last_log_term:  u64
│   │
│   └── RpcMessage::AppendEntries
│       ├── term:           u64
│       ├── leader_id:      NodeId
│       ├── prev_log_index: usize
│       ├── prev_log_term:  u64
│       ├── entries:        Vec<LogEntry>   ← empty = heartbeat
│       └── leader_commit:  usize
│
├── RpcResponse(RpcResponse)
│   ├── RpcResponse::RequestVoteResponse
│   │   ├── from:        NodeId
│   │   ├── term:        u64
│   │   └── vote_granted: bool
│   │
│   └── RpcResponse::AppendEntriesResponse
│       ├── from:        NodeId
│       ├── term:        u64
│       ├── success:     bool
│       └── match_index: usize   ← highest confirmed index on follower
│
├── Timeout(TimeoutKind)
│   ├── TimeoutKind::Election    ← fired by election timer task
│   └── TimeoutKind::Heartbeat  ← fired by heartbeat loop task
│
└── ClientCommand(String)        ← injected from outside (tests, main)


OutboundMsg  (returned by handlers)
├── Rpc      { to: NodeId, payload: RpcMessage  }  ← new outbound call
└── Response { to: NodeId, payload: RpcResponse }  ← reply to a call
```

---

## 9. Election Flow

Five nodes, node 4 fires its election timer first:

```
         Node 1       Node 2       Node 3       Node 4       Node 5
           │            │            │            │            │
   t=0     │            │            │     [timer fires]      │
           │            │            │            │            │
           │            │            │  term=0→1  │            │
           │            │            │  vote=self │            │
           │            │            │  role=Cand │            │
           │            │            │            │            │
           │◄── RequestVote(term=1) ─────────────┤            │
           │            │◄─────────────────────── │            │
           │            │            │◄─────────── │            │
           │            │            │            │ ──────────►│
           │            │            │            │            │
   [each checks: term ok? voted? log ok?]
           │            │            │            │            │
           ├─ VoteResp(granted=T) ──────────────►│            │
           │            ├─ VoteResp(granted=T) ──►│            │
           │            │            │            │◄── VoteResp(granted=T)
           │            │            │            │            │
           │            │            │    votes={4,1,2,5}=4    │
           │            │            │    4 > threshold(2) → WIN
           │            │            │            │            │
           │            │            │    role = LEADER        │
           │            │            │    next_index[all] = 1  │
           │            │            │    match_index[all] = 0  │
           │            │            │            │            │
           │◄── AppendEntries(term=1, entries=[]) ┤            │
           │            │◄────────────────────────┤            │
           │            │            │◄────────────┤            │
           │            │            │            ├──────────►│
           │            │            │     (heartbeat, all reset election timers)
```

---

## 10. Log Replication Flow

Client submits `"set x 42"` to the leader (node 4):

```
Client
  │
  │  ClientCommand("set x 42")
  │
  ▼
Node 4 (Leader)
  │
  │  handle_command():
  │    log.push(LogEntry { index:1, term:1, command:"set x 42" })
  │    return []   ← no immediate RPCs, waits for heartbeat
  │
  │  [50ms later — heartbeat fires]
  │
  │  handle_timeout(Heartbeat):
  │    for each peer:
  │      next = next_index[peer]    = 1
  │      prev_log_index = 0, prev_log_term = 0  (sentinel)
  │      entries = log[1..] = [LogEntry{1,1,"set x 42"}]
  │
  ├──── AppendEntries(term=1, prev=0/0, entries=[{1,1,"set x 42"}], commit=0) ────►  Node 1
  ├──── AppendEntries(...)  ────────────────────────────────────────────────────►  Node 2
  ├──── AppendEntries(...)  ────────────────────────────────────────────────────►  Node 3
  └──── AppendEntries(...)  ────────────────────────────────────────────────────►  Node 5

Each follower runs handle_rpc(AppendEntries):
  1. term ok? ✓
  2. consistency: log[0].term == 0 == prev_log_term ✓
  3. truncate(1), extend([{1,1,"set x 42"}])
  4. leader_commit(0) <= commit_index(0) — no advance yet
  5. return AppendEntriesResponse { success:true, match_index:1 }

  ◄──── AppendEntriesResponse(success=T, match_index=1) ─────────────────────────  Node 1
  ◄──── AppendEntriesResponse(success=T, match_index=1) ─────────────────────────  Node 2
  ◄──── AppendEntriesResponse(success=T, match_index=1) ─────────────────────────  Node 3

Node 4 processes each response via handle_response(AppendEntriesResponse):
  match_index[1] = 1, next_index[1] = 2
  try_advance_commit_index():
    N=1: log[1].term=1 == current_term=1 ✓
         replicas = 1(self) + 3(peers 1,2,3) = 4 > threshold(2) ✓
         commit_index = 1   ◄─── COMMITTED

  commit_tx.send(1)  ──────────────────────────────────► watch::Receiver (tests observe)

  [next heartbeat carries leader_commit=1]

  ◄──── AppendEntries(commit=1) ──────────────────────────────────────────────────  All nodes
  Each follower: leader_commit(1) > commit_index(0) → commit_index = 1
  commit_tx.send(1)  on each follower ──────────────────────────────────────────► watchers
```

---

## 11. Commit Index Advancement

`try_advance_commit_index()` — called after every successful AppendEntriesResponse:

```
Leader log (term=2, 5-node cluster, majority_threshold=2):

index:  [0]  [1]  [2]  [3]  [4]
term:    0    1    1    2    2
        sent sent sent sent sent
                        ↑
                  current_term=2

match_index state after two responses:
  peer2 = 4
  peer3 = 4
  peer4 = 0
  peer5 = 0

Scan from N=4 down to commit_index+1:

  N=4: log[4].term=2 == current_term=2 ✓
       replicas = 1(self) + 2(peer2,3 have match_index>=4) = 3
       3 > majority_threshold(2) ✓
       → commit_index = 4   STOP (descending, first hit = highest safe N)

  N=3 would also pass but we already found N=4 and broke.

Figure 8 check — N=2: log[2].term=1 ≠ current_term=2
  → SKIP (even though replicas would be enough)
  → N=2 is committed implicitly when N=4 commits (it's in the prefix)
```

---

## 12. Handler Decision Trees

### handle_rpc — RequestVote

```
RequestVote { term, candidate_id, last_log_index, last_log_term }
│
├─ step_down_if_stale(term)           ← ALWAYS FIRST
│
├─ term < current_term?
│   YES → respond vote_granted=false  ← candidate is stale
│
├─ voted_for == None OR voted_for == candidate_id?  → can_vote
│
├─ is_log_up_to_date(last_log_index, last_log_term)? → log_ok
│   ├─ candidate_last_term > my_last_term  → true  (higher term wins)
│   ├─ candidate_last_term < my_last_term  → false
│   └─ equal terms: candidate_last_index >= my_last_index → true/false
│
├─ can_vote AND log_ok?
│   YES → voted_for = candidate_id
│          respond vote_granted=true
│          [event loop resets election timer]
│   NO  → respond vote_granted=false
```

### handle_rpc — AppendEntries

```
AppendEntries { term, prev_log_index, prev_log_term, entries, leader_commit }
│
├─ step_down_if_stale(term)
│
├─ term < current_term?
│   YES → respond success=false
│
├─ role == Candidate?
│   YES → role = Follower  (someone else won, we lost)
│
├─ log[prev_log_index].term == prev_log_term?
│   NO  → respond success=false  (leader will back up next_index)
│
├─ log.truncate(prev_log_index + 1)   ← delete conflicts
├─ log.extend(entries)                ← append new
│
├─ leader_commit > commit_index?
│   YES → commit_index = min(leader_commit, new_last_index)
│
└─ respond success=true, match_index=new_last_index
```

### handle_response — AppendEntriesResponse

```
AppendEntriesResponse { from, term, success, match_index: peer_match }
│
├─ step_down_if_stale(term)
│
├─ success == true?
│   YES → match_index[from] = peer_match
│          next_index[from]  = peer_match + 1
│          try_advance_commit_index()   ← may advance commit_index
│
└─ success == false?
       next_index[from] -= 1   ← back up, retry with longer prefix
```

### handle_timeout — Election

```
Timeout(Election)
│
├─ current_term += 1              ← MUST happen before any network traffic
├─ voted_for = self
├─ role = Candidate { votes_received: {self} }
│
└─ for each peer:
       send RequestVote { term, candidate_id=self,
                          last_log_index, last_log_term }
```

---

## 13. Timer Architecture

```
         NodeRunner.run()
               │
               │ reset_election_timer() called on:
               │   • startup
               │   • step down from Leader
               │   • vote granted
               │   • AppendEntries accepted
               │
         ┌─────▼──────────────────────────────────────┐
         │         Election Timer Task                 │
         │                                             │
         │  abort old task (if any)                    │
         │  delay = rand(150ms..=300ms)                │
         │                                             │
         │  tokio::spawn:                              │
         │    sleep(delay).await                       │
         │    self_tx.send(Timeout(Election))          │
         │                                             │
         │  store AbortHandle                          │
         └─────────────────────────────────────────────┘

         NodeRunner.run() — on becoming Leader:
               │
         ┌─────▼──────────────────────────────────────┐
         │        Heartbeat Timer Task                 │
         │                                             │
         │  tokio::spawn:                              │
         │    loop {                                   │
         │      sleep(50ms).await                      │
         │      self_tx.send(Timeout(Heartbeat))       │
         │      if send fails → break  (node shutdown) │
         │    }                                        │
         │                                             │
         │  store AbortHandle                          │
         └─────────────────────────────────────────────┘

Timer lifecycle:
  ┌──────────────────────────────────────────────────┐
  │  Follower/Candidate   │   Leader                 │
  │  election_timer: Some │   election_timer: None    │
  │  heartbeat_timer: None│   heartbeat_timer: Some   │
  └──────────────────────────────────────────────────┘

  Role transition → always swap timers:
    Follower→Leader:   cancel election, start heartbeat
    Leader→Follower:   cancel heartbeat, start election
```

---

## 14. Figure 8 Safety

The subtle rule that prevents committed entries from being overwritten.

```
Without the rule — the dangerous scenario:

Step 1: 5 nodes. Node 1 is leader in term 1.
        Replicates entry [index=1, term=1] to Node 2 only, then crashes.

        Node1:[S,1]  Node2:[S,1]  Node3:[S]  Node4:[S]  Node5:[S]
        (S = sentinel)

Step 2: Node 5 wins election in term 2 (its log is [S], still "up to date"
        because term 2 > term 1).  Node 5 replicates [index=1, term=2].

        Node1:[S,1]  Node2:[S,1]  Node3:[S,2]  Node4:[S,2]  Node5:[S,2]

Step 3: Node 1 recovers and wins election in term 3 (has longer log).
        Node 1 replicates its old [index=1, term=1] to Node 2, 3, 4 — now
        it's on a MAJORITY.

        If Node 1 now commits [index=1, term=1] by counting replicas...

Step 4: Node 5 wins election in term 4 (its last entry is term=2 which beats
        Node 1's term=1 in the up-to-date check).
        Node 5 overwrites [index=1] with [term=2] on all nodes.

RESULT: [index=1, term=1] was committed but then OVERWRITTEN. Safety violated.

─────────────────────────────────────────────────────────────────────────

With the rule — how our code prevents it:

In try_advance_commit_index():

  for n in (commit_index+1 ..= last).rev() {
      if log[n].term != current_term { continue; }  ← THE GUARD
      ...
  }

Back to Step 3: Node 1 is leader in term 3. It has [index=1, term=1].
  log[1].term = 1  ≠  current_term = 3  → SKIPPED, never committed directly.

Node 1 must first write a term=3 entry and replicate it to a majority.
When THAT commits, [index=1] is committed implicitly as part of the prefix.

If Node 5 then wins term 4, its up-to-date check (term=3 > term=2) now
FAILS — Node 5's log ends with term=2 which is older than Node 1's term=3.
Node 5 cannot win the election. Safety preserved.
```

---

## 15. Test Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        cargo test                                    │
│                                                                      │
│  ┌────────────────────────────────────────┐                          │
│  │  src/lib.rs  (35 unit tests)           │                          │
│  │  handlers::tests module                │                          │
│  │                                        │                          │
│  │  make_node(id, peers) ──► RaftNode     │                          │
│  │  make_leader(id, peers, term)          │                          │
│  │  push_entry(node, term, cmd)           │                          │
│  │                                        │                          │
│  │  Call handle_message(crafted_msg)      │                          │
│  │  Assert on: returned Vec<OutboundMsg>  │                          │
│  │             mutated node state         │                          │
│  │                                        │                          │
│  │  NO runtime, NO channels*, NO async    │                          │
│  │  * mpsc::channel(1) created but never  │                          │
│  │    used — inbox is never read          │                          │
│  └────────────────────────────────────────┘                          │
│                                                                      │
│  ┌────────────────────────────────────────┐                          │
│  │  tests/cluster_integration.rs (6 tests)│                          │
│  │                                        │                          │
│  │  spawn_cluster(n)                      │                          │
│  │    → real nodes, real timers           │                          │
│  │    → real mpsc channels                │                          │
│  │    → real election + replication       │                          │
│  │                                        │                          │
│  │  broadcast(senders, cmd)               │                          │
│  │    → ClientCommand to all nodes        │                          │
│  │    → only leader appends               │                          │
│  │                                        │                          │
│  │  all_nodes_commit(watches, n, timeout) │                          │
│  │    → watch::Receiver::wait_for(≥n)     │                          │
│  │    → asserts ALL 5 nodes committed     │                          │
│  │                                        │                          │
│  │  #[tokio::test]  (current_thread)      │                          │
│  └────────────────────────────────────────┘                          │
│                                                                      │
│  Unit tests    : pure logic, no time, deterministic                  │
│  Integ tests   : real async, real timers, non-deterministic order    │
└──────────────────────────────────────────────────────────────────────┘

Integration test scenarios:
  ┌────────────────────────────────────────────────────────────┐
  │ cluster_elects_leader_and_commits_one_command              │
  │   5 nodes → wait 500ms → 1 cmd → all nodes commit[1]      │
  ├────────────────────────────────────────────────────────────┤
  │ all_nodes_commit_three_commands_in_order                   │
  │   5 nodes → 3 cmds → all nodes commit[3]                  │
  ├────────────────────────────────────────────────────────────┤
  │ three_node_cluster_commits                                 │
  │   3 nodes (minimum) → 1 cmd → all nodes commit[1]         │
  ├────────────────────────────────────────────────────────────┤
  │ cluster_commits_ten_commands                               │
  │   5 nodes → 10 cmds (5ms apart) → all commit[10]          │
  ├────────────────────────────────────────────────────────────┤
  │ rapid_burst_of_commands_all_commit                         │
  │   5 nodes → 5 cmds (no sleep) → all commit[5]             │
  ├────────────────────────────────────────────────────────────┤
  │ cluster_recovers_and_commits_after_reelection              │
  │   commit[1] → wait 600ms → commit[2] (heartbeats prevent  │
  │   spurious re-election during the wait)                    │
  └────────────────────────────────────────────────────────────┘
```

---

## 16. Full Message Lifecycle

End-to-end: client command → committed on all 5 nodes.

```
EXTERNAL
  │
  │  senders[3].send(ClientCommand("set x 42"))
  │
  ▼
Node 4 inbox
  │
  ▼
NodeRunner::run() — iteration N
  │
  ├─ snapshot: was_leader=true, commit_before=0
  │
  ├─ handle_message(ClientCommand("set x 42"))
  │     └─ handle_command("set x 42")
  │           log.push({index:1, term:1, cmd:"set x 42"})
  │           return []
  │
  ├─ no role change, no commit change
  ├─ dispatch_all([])  — nothing to send
  └─ no timer change

[50ms later — heartbeat task fires]

Node 4 inbox receives Timeout(Heartbeat)
  │
NodeRunner::run() — iteration N+1
  │
  ├─ handle_message(Timeout(Heartbeat))
  │     └─ handle_timeout(Heartbeat)
  │           send_heartbeats()
  │             for each peer (1,2,3,5):
  │               next=1, prev=0/term0, entries=[{1,1,"set x 42"}]
  │               → OutboundMsg::Rpc { to: peer, AppendEntries{...} }
  │           return [Rpc×4]
  │
  ├─ dispatch_all([Rpc×4]):
  │     peers[1].send(Rpc{from:4, AppendEntries{...}})
  │     peers[2].send(Rpc{from:4, AppendEntries{...}})
  │     peers[3].send(Rpc{from:4, AppendEntries{...}})
  │     peers[5].send(Rpc{from:4, AppendEntries{...}})
  │
  └─ no role/commit change, no timer change

[concurrently — Nodes 1,2,3,5 each process the AppendEntries]

Node 1 inbox receives Rpc{from:4, AppendEntries{...}}
  │
NodeRunner::run()
  │
  ├─ handle_message(Rpc{AppendEntries})
  │     └─ handle_rpc(from=4, AppendEntries)
  │           consistency check ✓
  │           log.extend([{1,1,"set x 42"}])
  │           leader_commit=0 ≤ commit_index=0 — no advance
  │           return [Response{to:4, AppendEntriesResponse{success:T, match:1}}]
  │
  ├─ dispatch_all([Response]):
  │     peers[4].send(RpcResponse(AppendEntriesResponse{success:T,match:1}))
  │
  └─ reset election timer (accepted_ae=true)

[Nodes 2,3,5 do the same concurrently]

Node 4 inbox receives RpcResponse×4 (one per follower)
Processing first response (from Node 1):
  │
NodeRunner::run()
  │
  ├─ handle_message(RpcResponse(AppendEntriesResponse{from:1,success:T,match:1}))
  │     └─ handle_response(AER)
  │           match_index[1]=1, next_index[1]=2
  │           try_advance_commit_index():
  │             N=1: term=1==current=1 ✓
  │             replicas = 1(self)+1(peer1) = 2, 2>2? NO
  │             commit_index stays 0
  │           return []
  │
Processing second response (from Node 2):
  │
  ├─ match_index[2]=1, next_index[2]=2
  │   try_advance_commit_index():
  │     N=1: replicas = 1+2 = 3, 3>2? YES
  │     commit_index = 1   ◄─── COMMITTED
  │
  ├─ commit_tx.send(1)  ─────────────────────────────► watch::Receiver[4]
  │                                                    (test watcher unblocks)
  │
  └─ eprintln!("[node 4] committed log[1..=1]")

[next heartbeat from Node 4 carries leader_commit=1]
[Nodes 1,2,3,5 each advance their commit_index to 1]
[Each fires commit_tx.send(1)]
[All 5 watch::Receivers unblock]
[Integration test all_nodes_commit() returns — test passes]
```

---

*Reference: Raft paper https://raft.github.io/raft.pdf*
*§3 (basics) · §5 (leader election) · §6 (log replication) · §7 (safety)*
