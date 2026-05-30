# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project

A real Raft consensus algorithm in Rust with a real-time Next.js visualizer.
Every piece of Raft logic is real — no shortcuts, no simulations.

Deep explanation of the algorithm, all data structures, and correctness rules: `RAFT_EXPLAINER.md`.

---

## Commands

### Rust backend (`backend/`)

```bash
cargo build                          # compile
cargo check                          # type-check only (faster)
cargo clippy                         # lints
cargo test                           # run all tests (unit + integration)
cargo test <name>                    # run a single test by name substring
cargo test --test cluster_integration # integration tests only
cargo test --lib                     # unit tests only (handlers::tests)
cargo run                            # run the demo cluster (main.rs)
```

### TypeScript reference (`Raft-Consensus-Algo-TS-/raftImpl/`)

Working reference implementation — useful for cross-checking behavior, not for production.

```bash
cd Raft-Consensus-Algo-TS-/raftImpl
npm install
npm run test:3node   # 3-node cluster
npm run test:5node   # 5-node cluster
```

---

## Source layout

```
src/
├── lib.rs         — crate root; declares all modules pub (enables tests/ imports)
├── main.rs        — Axum server: spawns 5-node cluster, WebSocket + REST endpoints
├── state.rs       — NodeId, Role, LogEntry, RaftNode (pure data, no async)
├── message.rs     — Message, RpcMessage, RpcResponse, TimeoutKind, OutboundMsg
├── handlers.rs    — impl RaftNode: all four handlers + helpers + 35 unit tests
├── node.rs        — NodeRunner (async event loop, timers), spawn_cluster()
├── events.rs      — StateEvent enum (WebSocket-serializable events)
├── ws.rs          — WebSocket handler (GET /ws)
├── http.rs        — REST handlers (POST /command, /crash/:id, /restart/:id)
└── app_state.rs   — Axum shared state (event_tx, node_senders, crashed set)

tests/
└── cluster_integration.rs  — 8 end-to-end tests (6 Raft + 2 WebSocket serialization)

frontend/
├── app/page.tsx            — main UI: node grid, command input, event feed
├── components/NodeCard.tsx — role-colored cards with crash/restart buttons
├── components/EventFeed.tsx — live scrolling event stream
├── hooks/useRaftCluster.ts — WebSocket connection + cluster state reducer
└── types/raft.ts           — TypeScript types matching backend StateEvent
```

---

## Architecture

### Layering rule

`state.rs` + `message.rs` + `handlers.rs` form the **pure Raft layer** — no Tokio,
no async, no networking. `node.rs` is the **async shell** that wraps it.
This separation is what makes the 35 unit tests runnable without any runtime.

### How a message flows

```
Timer task ──Timeout(Election)──► NodeRunner.inbox
                                         │
                            handle_message(&mut RaftNode)    ← pure, sync
                                         │
                              Vec<OutboundMsg> returned
                                         │
                            dispatch_all() → peer inboxes
                                         │
                            timer management (reset / switch)
```

Handlers **never** touch channels, timers, or async. They mutate `RaftNode`
and return messages. `NodeRunner.run()` owns all I/O.

### Role carries its state

`Role::Leader { next_index, match_index }` — the compiler makes `next_index`
and `match_index` unreachable outside a `Role::Leader { .. }` pattern match.
No runtime role-guards anywhere.

### Observability

`spawn_cluster(n)` returns:
```rust
(
  Vec<Sender<Message>>,              // command injection (POST /command)
  Vec<watch::Receiver<usize>>,       // commit_index watchers (integration tests)
  broadcast::Sender<StateEvent>,     // WebSocket event feed
  Arc<RwLock<HashSet<NodeId>>>,      // crashed nodes set
)
```

- **Senders** — inject commands and control messages (Restart).
- **Watch receivers** — integration tests wait for `commit_index` convergence.
- **Event broadcast** — real-time WebSocket feed; subscribers get every StateEvent.
- **Crashed set** — tracks which nodes are "down"; they skip all message processing.

### Timer design

Each node runs two independent Tokio tasks:

| Timer | Behaviour | Reset trigger |
|---|---|---|
| Election | One-shot, random [150–300 ms], fires `Timeout(Election)` | Valid AppendEntries accepted, vote granted, or role change away from Leader |
| Heartbeat | Infinite loop, fires `Timeout(Heartbeat)` every 50 ms | Replaced on every `become_leader()` call |

Reset = `abort_handle.abort()` + spawn new task. Handles live in `NodeRunner`, not on `RaftNode`.

### Log sentinel

`log[0]` is always `LogEntry { index: 0, term: 0, command: "" }`. This makes
`prev_log_index=0` always a valid consistency-check anchor. Never remove it.

### Figure 8 / commit safety

`try_advance_commit_index` skips any index `N` where `log[N].term != current_term`.
A leader never commits prior-term entries by counting replicas alone — they commit
implicitly when the first current-term entry clears the majority threshold.

---

## Build order

1. ✅ Data structures — `state.rs`, `message.rs`
2. ✅ Handler functions — pure, sync, no timers
3. ✅ Unit tests — 35 tests in `handlers::tests`
4. ✅ Async event loop — `node.rs`, timer tasks, `spawn_cluster`
5. ✅ Integration tests — `tests/cluster_integration.rs`, watch-channel observability
6. ☐ WebSocket bridge — `StateEvent` broadcast channel, Axum `/ws` endpoint
7. ☐ Next.js frontend — connect WS, render node circles, log panel, controls
8. ☐ Frontend interactions — crash/restart, network partition, speed control

---

## Invariants — never violate

- No Raft logic in the event loop — `NodeRunner.run()` routes and dispatches only.
- Never access `next_index` / `match_index` outside `Role::Leader { .. }` match.
- Never commit prior-term entries directly (Figure 8).
- Never `unwrap()` on channel sends — peer may be shut down.
- One `RaftNode` per Tokio task, never shared across threads.
- Never remove `log[0]` sentinel.

---

## WebSocket event shape (Step 6)

```typescript
type StateEvent =
  | { type: "role_change";    node_id: number; role: "Follower"|"Candidate"|"Leader"; term: number }
  | { type: "log_append";     node_id: number; entry: { index: number; term: number; command: string } }
  | { type: "commit";         node_id: number; commit_index: number }
  | { type: "vote_cast";      node_id: number; voted_for: number; term: number }
  | { type: "vote_received";  node_id: number; from: number; granted: boolean }
  | { type: "heartbeat";      leader_id: number; term: number }
  | { type: "election_start"; node_id: number; term: number }
```

Raft logic never imports WebSocket or Axum. It emits to a
`broadcast::Sender<StateEvent>`; the bridge subscribes.
