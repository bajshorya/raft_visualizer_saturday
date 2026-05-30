// ── events.rs — observable state events emitted by the Raft cluster ──────────
//
// Every time the cluster state changes in a way the frontend cares about,
// NodeRunner emits one of these.  They travel through a broadcast channel to
// the WebSocket bridge, which serialises them to JSON and sends them to every
// connected browser.
//
// Raft logic (handlers.rs) never imports this module — it is purely a
// concern of the async shell (node.rs) that wraps the state machine.

use serde::Serialize;

// ── StateEvent ────────────────────────────────────────────────────────────────
//
// #[serde(tag = "type", rename_all = "snake_case")] produces JSON like:
//   { "type": "role_change", "node_id": 3, "role": "Leader", "term": 2 }
//
// This matches the TypeScript StateEvent union defined in CLAUDE.md.

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StateEvent {
    // A node transitioned between Follower / Candidate / Leader.
    RoleChange {
        node_id: u64,
        role:    RoleLabel,
        term:    u64,
    },

    // A new entry was appended to a node's log (may be uncommitted).
    LogAppend {
        node_id: u64,
        entry:   EntrySnapshot,
    },

    // A node's commit_index advanced — entries up to this index are durable.
    Commit {
        node_id:      u64,
        commit_index: usize,
    },

    // A node granted its vote to another node this term.
    VoteCast {
        node_id:   u64,
        voted_for: u64,
        term:      u64,
    },

    // A Candidate received a vote response from a peer.
    VoteReceived {
        node_id: u64,
        from:    u64,
        granted: bool,
    },

    // A follower accepted an AppendEntries RPC — evidence of an active leader.
    Heartbeat {
        leader_id: u64,
        term:      u64,
    },

    // A node's election timer fired and it started a new election.
    ElectionStart {
        node_id: u64,
        term:    u64,
    },

    // A RequestVote RPC was sent from one node to another.
    MessageSent {
        from:         u64,
        to:           u64,
        message_type: MessageType,
    },
}

// ── Supporting types ──────────────────────────────────────────────────────────

// JSON: "Follower" | "Candidate" | "Leader"  (matches the TypeScript union literal)
#[derive(Debug, Clone, Serialize)]
pub enum RoleLabel {
    Follower,
    Candidate,
    Leader,
}

// Snapshot of one log entry — embedded in LogAppend events.
#[derive(Debug, Clone, Serialize)]
pub struct EntrySnapshot {
    pub index:   usize,
    pub term:    u64,
    pub command: String,
}

// Type of message being sent — used for arrow color/label in the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
}
