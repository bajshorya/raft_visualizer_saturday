# Raft Consensus Visualizer

A real-time visualization of the Raft consensus algorithm implemented in Rust with a Next.js frontend.

![Preview](https://img.shields.io/badge/Rust-1.75+-orange.svg)
![Preview](https://img.shields.io/badge/Next.js-14-black.svg)
![Preview](https://img.shields.io/badge/License-MIT-blue.svg)

## 🎯 Features

- **Real Raft Implementation** - Not a simulation. Every handler, timer, and state machine follows the Raft paper exactly
- **Live Visualization** - WebSocket-powered real-time updates showing elections, log replication, and message flow
- **Interactive Controls** - Crash/restart nodes, send commands, watch leader re-elections
- **Message Flow Animation** - See RequestVote and AppendEntries RPCs flying between nodes
- **Pure/Async Separation** - Clean architecture with testable synchronous Raft core + async I/O shell
- **43 Tests** - 35 unit tests + 8 integration tests covering all Raft scenarios

## 🚀 Quick Start

### Option 1: Docker (Easiest)
```bash
docker-compose up --build
```
Open http://localhost:3000

### Option 2: Quick Start Script
```bash
./start.sh
```

### Option 3: Manual
**Terminal 1 - Backend:**
```bash
cargo run
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:3000

## 📋 Requirements

- **Rust** 1.75+ ([Install](https://rustup.rs/))
- **Node.js** 20+ ([Install](https://nodejs.org/))
- **Docker** (optional, for containerized deployment)

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Next.js Frontend                     │
│  (WebSocket client, node cards, event feed, controls)  │
└────────────────┬────────────────────────────────────────┘
                 │ WebSocket + REST
┌────────────────▼────────────────────────────────────────┐
│              Axum HTTP Server (Rust)                    │
│  • GET  /ws         → WebSocket event stream            │
│  • POST /command    → Submit client commands            │
│  • POST /crash/:id  → Crash a node                      │
│  • POST /restart/:id → Restart a crashed node           │
└────────────────┬────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────┐
│           5-Node Raft Cluster (In-Process)              │
│  ┌──────────────────────────────────────────────────┐   │
│  │  NodeRunner (async shell)                        │   │
│  │  • Event loop + timer management                 │   │
│  │  • Dispatches to pure RaftNode                   │   │
│  │  • Emits StateEvents to broadcast channel        │   │
│  └──────────────┬───────────────────────────────────┘   │
│                 │                                        │
│  ┌──────────────▼───────────────────────────────────┐   │
│  │  RaftNode (pure, sync, no async/channels)       │   │
│  │  • handle_rpc (RequestVote, AppendEntries)      │   │
│  │  • handle_response (vote counting, log sync)    │   │
│  │  • handle_timeout (elections, heartbeats)       │   │
│  │  • handle_command (client requests)             │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Key Design Decisions

**Pure/Async Separation**  
The Raft state machine (`handlers.rs`) is pure Rust with zero async/Tokio. This makes it:
- Trivially unit testable (no runtime needed)
- Easy to reason about (no hidden concurrency)
- Portable (could run in browser via WASM)

**Role Carries State**  
```rust
enum Role {
    Follower,
    Candidate { votes_received: HashSet<NodeId> },
    Leader { next_index: HashMap<...>, match_index: HashMap<...> },
}
```
The compiler enforces that `next_index` is unreachable unless you're a Leader. No runtime guards needed.

**Timer Architecture**  
Each node runs two independent Tokio tasks:
- **Election timer**: random 150-300ms, fires if no heartbeat received
- **Heartbeat timer**: fixed 50ms loop (leaders only)

Resets happen via `AbortHandle` - clean cancellation, no channels.

**Figure 8 Commit Safety**  
Leaders never commit prior-term entries by counting replicas. They commit implicitly when the first current-term entry reaches majority.

## 🧪 Testing

```bash
# All tests (unit + integration)
cargo test

# Unit tests only (handlers, 35 tests)
cargo test --lib

# Integration tests (live cluster, 8 tests)
cargo test --test cluster_integration

# Single test
cargo test election_timeout_becomes_candidate
```

## 📁 Project Structure

```
backend/
├── src/
│   ├── state.rs      - Core data structures (NodeId, Role, LogEntry, RaftNode)
│   ├── message.rs    - RPC messages and responses
│   ├── handlers.rs   - Pure Raft logic (4 handlers + helpers + 35 tests)
│   ├── node.rs       - Async shell (timers, event loop, spawn_cluster)
│   ├── events.rs     - StateEvent enum for WebSocket
│   ├── ws.rs         - WebSocket handler
│   ├── http.rs       - REST endpoints
│   ├── app_state.rs  - Axum shared state
│   └── main.rs       - Entry point
├── tests/
│   └── cluster_integration.rs - End-to-end cluster tests
├── frontend/
│   ├── app/page.tsx           - Main UI
│   ├── components/
│   │   ├── NodeCard.tsx       - Node visualization
│   │   ├── EventFeed.tsx      - Live event stream
│   │   └── MessageArrows.tsx  - Animated RPC visualization
│   ├── hooks/useRaftCluster.ts - WebSocket + state management
│   └── types/raft.ts          - TypeScript types
├── Cargo.toml
├── docker-compose.yml
└── DEPLOYMENT.md      - Full deployment guide
```

## 🎮 Usage

### Send Commands
Type in the command input:
```
set x 42
del y
```
The leader appends it to its log and replicates to followers.

### Crash/Restart Nodes
Click the **× CRASH** button on any node card. If it was the leader:
1. Followers timeout after 150-300ms
2. They start an election
3. One wins majority and becomes new leader
4. Click **↻ RESTART** to bring the node back

### Watch Message Flow
Animated arrows show RPCs:
- **Amber dashed** - RequestVote
- **Amber dotted** - RequestVoteResponse  
- **Cyan solid** - AppendEntries
- **Cyan dotted** - AppendEntriesResponse

### Event Feed
Filtered to show only important events:
- Role changes (→ LEADER, → CANDIDATE, → FOLLOWER)
- Elections starting
- Log appends
- Commits

Noisy events (heartbeats, individual votes) are filtered out.

## 🔧 Configuration

### Backend Port
Edit `src/main.rs`:
```rust
let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
```

### Frontend URLs
Create `frontend/.env.local`:
```bash
NEXT_PUBLIC_WS_URL=ws://localhost:3001/ws
NEXT_PUBLIC_HTTP_URL=http://localhost:3001
```

### Timer Intervals
Edit `src/node.rs`:
```rust
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_MAX_MS: u64 = 300;
const HEARTBEAT_MS: u64 = 50;
```

## 📚 Documentation

- **[CLAUDE.md](CLAUDE.md)** - Project overview, commands, architecture, invariants
- **[RAFT_EXPLAINER.md](RAFT_EXPLAINER.md)** - Deep dive into Raft algorithm, data structures, correctness rules
- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Production deployment guide (VPS, Docker, cloud platforms)
- **[DIAGRAMS.md](DIAGRAMS.md)** - Visual diagrams of workflows and processes

## 🐛 Troubleshooting

**Port already in use:**
```bash
lsof -i :3001  # Find process
kill -9 <PID>  # Kill it
```

**WebSocket won't connect:**
- Check CORS is permissive in `main.rs`
- Verify backend is running on 3001
- Check browser console for errors

**Frontend shows stale data:**
- Refresh the page (WebSocket connects after initial election)
- Heartbeat events update leader state for late joiners

## 🤝 Contributing

1. Read the [Raft paper](https://raft.github.io/raft.pdf)
2. Check `RAFT_EXPLAINER.md` for implementation details
3. Run tests: `cargo test`
4. Follow the invariants in `CLAUDE.md`

## 📄 License

MIT License - see LICENSE file

## 🙏 Acknowledgments

- [Raft Consensus Algorithm](https://raft.github.io/) by Diego Ongaro and John Ousterhout
- TypeScript reference implementation in `Raft-Consensus-Algo-TS-/`

## 🔗 Links

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)
- [Rust Documentation](https://doc.rust-lang.org/)
- [Tokio Async Runtime](https://tokio.rs/)
- [Next.js Documentation](https://nextjs.org/docs)
